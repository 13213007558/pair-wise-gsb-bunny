# frozen_string_literal: true

module Bunny
  class SendScheduler
    DEFAULT_CHANNEL_BYTE_BUDGET = 65_536

    class Job
      attr_accessor :index, :offset, :error, :generation, :priority
      attr_reader :channel, :fragments, :signal_activity

      def initialize(channel:, fragments:, generation:, priority:, signal_activity:)
        @channel = channel
        @fragments = fragments
        @generation = generation
        @priority = priority
        @signal_activity = signal_activity
        @index = 0
        @offset = 0
        @error = nil
        @complete = false
      end

      def complete?
        @complete
      end

      def complete!
        @complete = true
      end

    end

    def initialize(transport_getter, byte_budget, activity_callback = nil, error_callback = nil)
      @transport_getter = transport_getter
      @byte_budget = byte_budget
      @activity_callback = activity_callback
      @error_callback = error_callback
      @mutex = Mutex.new
      @condition = ConditionVariable.new

      @generation = 0
      @writer = nil
      @running = false
      @queues = Hash.new { |hash, key| hash[key] = [] }
      @body_bytes_written = Hash.new(0)
      @order = []
      @exhausted_body_channels = []
    end

    def start
      @mutex.synchronize do
        return if @writer

        @running = true
        thread = Thread.new { run }
        thread.report_on_exception = false
        @writer = thread
      end
    end

    def stop
      writer = nil
      @mutex.synchronize do
        @running = false
        writer = @writer
        @writer = nil
        @condition.broadcast
      end
      writer&.join
    end

    def stop_async
      @mutex.synchronize do
        @running = false
        @writer = nil
        @condition.broadcast
      end
    end

    def reset
      jobs = nil
      @mutex.synchronize do
        @generation += 1
        jobs = drain_locked
        @condition.broadcast
      end
      abort_jobs(jobs, ConnectionClosedError.new("connection was reset"))
      @generation
    end

    def send_frame(frame, signal_activity: true)
      job = build_job(frame.channel, [frame.encode], true, signal_activity)
      enqueue_and_wait(job, true)
    end

    def write_heartbeat
      data = AMQ::Protocol::HeartbeatFrame.encode
      job = build_job(0, [data], true, false)
      enqueue_and_wait(job, true)
    end

    def send_frames(frames, signal_activity: true, wait: true)
      encoded = frames.map(&:encode)
      channel = frames.first.channel
      job = build_job(channel, encoded, false, signal_activity)
      enqueue_and_wait(job, wait)
    end

    private

    def build_job(channel, fragments, priority, signal_activity)
      Job.new(channel: channel,
              fragments: fragments,
              generation: @generation,
              priority: priority,
              signal_activity: signal_activity)
    end

    def enqueue_and_wait(job, wait)
      @mutex.synchronize do
        job.priority = false unless job.priority || job.channel.zero? || !@queues.key?(job.channel)
        enqueue_locked(job)
        @condition.signal
      end

      if wait
        @mutex.synchronize do
          @condition.wait(@mutex) until job.complete?
          raise job.error if job.error
        end
      end
    end

    def run
      loop do
        job = next_job
        break unless job
        write_job(job)
      end
    rescue => e
      @mutex.synchronize { @running = false }
      @error_callback.call(e) if @error_callback
    end

    def next_job
      @mutex.synchronize do
        loop do
          return nil unless @running

          job = select_job
          return job if job

          @condition.wait(@mutex)
        end
      end
    end

    def select_job
      partial = @order.lazy.map { |channel| @queues[channel]&.first }.find { |job| job&.offset.to_i.positive? }
      return partial if partial

      @order.each do |channel|
        job = @queues[channel]&.first
        return job if job&.priority
      end

      channels = @order.select { |channel| @queues[channel]&.any? }
      return nil if channels.empty?

      candidates = channels.select do |channel|
        job = @queues[channel].first
        job && writable?(job)
      end

      if candidates.empty?
        return nil
      end

      channel = candidates.min_by { |candidate| [@body_bytes_written[candidate], candidate] }
      @queues[channel].first
    end

    def writable?(job)
      fragment = job.fragments[job.index]
      return true if job.offset.positive? || job.priority || !body_fragment?(fragment)

      @body_bytes_written[job.channel] < @byte_budget
    end

    def body_fragment?(fragment)
      fragment.getbyte(0) == AMQ::Protocol::Frame::TYPES[:body]
    end

    def write_job(job)
      fragment = job.fragments[job.index]
      limit = @mutex.synchronize do
        return finish(job, ConnectionClosedError.new(fragment)) if job.generation != @generation

        fragment_length = fragment.bytesize - job.offset
        if body_fragment?(fragment)
          [@byte_budget - @body_bytes_written[job.channel], fragment_length].min
        else
          fragment_length
        end
      end
      count = @transport_getter.call.write_some(fragment, job.offset, limit)

      complete_fragment = false
      complete_job = false
      @mutex.synchronize do
        if job.generation != @generation
          finish(job, ConnectionClosedError.new("connection was reset"))
          return
        end

        job.offset += count
        @body_bytes_written[job.channel] += count if body_fragment?(fragment)

        complete_fragment = job.offset >= fragment.bytesize
        if complete_fragment
          job.index += 1
          job.offset = 0
          complete_job = job.index >= job.fragments.length
          if complete_job
            dequeue_locked(job)
            @activity_callback.call if job.signal_activity && @activity_callback
            finish(job)
          end
          next_fragment = job.fragments[job.index]
          if next_fragment && body_fragment?(next_fragment) &&
             @body_bytes_written[job.channel] >= @byte_budget
            @exhausted_body_channels << job.channel unless @exhausted_body_channels.include?(job.channel)
          end
          reset_exhausted_body_channels_if_round_complete
        end
        @condition.signal if complete_fragment
      end
    rescue => e
      @mutex.synchronize { finish(job, e) }
    end

    def enqueue_locked(job)
      body_fragment = body_fragment?(job.fragments[job.index])
      unless @order.include?(job.channel)
        @order << job.channel
      end
      @queues[job.channel] << job
      if body_fragment && @exhausted_body_channels.any?
        @exhausted_body_channels.delete(job.channel)
        @condition.signal
      end
    end

    def dequeue_locked(job)
      queue = @queues[job.channel]
      queue.shift
      return unless queue.empty?

      @queues.delete(job.channel)
      @order.delete(job.channel)
    end

    def drain_locked
      jobs = @order.flat_map { |channel| @queues[channel] || [] }
      @queues.clear
      @order.clear
      @body_bytes_written.clear
      @exhausted_body_channels.clear
      jobs
    end

    def abort_jobs(jobs, error)
      jobs.each { |job| finish(job, error) }
    end

    def finish(job, error = nil)
      job.error = error
      job.complete!
      @condition.broadcast
    end

    def reset_exhausted_body_channels_if_round_complete
      active = @order.select { |channel| @queues[channel]&.any? }
      return if active.length <= 1
      return unless active.any? && active.all? { |channel| @exhausted_body_channels.include?(channel) }

      @body_bytes_written.clear
      @exhausted_body_channels.clear
    end

  end
end
