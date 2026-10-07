# frozen_string_literal: true

module Bunny
  # @private
  class WriteScheduler
    METHOD_FRAME = 1
    HEADER_FRAME = 2
    BODY_FRAME = 3

    class Job
      attr_reader :channel, :segments, :control

      def initialize(channel, segments, control)
        @channel = channel
        @segments = segments
        @control = control
        @mutex = Monitor.new
        @condition = @mutex.new_cond
        @done = false
        @error = nil
      end

      def fail(error)
        @mutex.synchronize do
          return if @done

          @done = true
          @error = error
          @condition.broadcast
        end
      end

      def complete
        @mutex.synchronize do
          @done = true
          @condition.broadcast
        end
      end

      def wait
        @mutex.synchronize do
          @condition.wait_while { !@done }
          raise @error if @error
        end
      end
    end

    def initialize(write, byte_budget)
      @write = write
      @byte_budget = byte_budget
      @mutex = Monitor.new
      @condition = @mutex.new_cond
      @queues = Hash.new { |hash, channel| hash[channel] = [] }
      @round = []
      @sent = Hash.new(0)
      @active_channel = nil
      @running = false
      @thread = nil
    end

    def start
      @mutex.synchronize do
        return if @running

        @running = true
        @thread = Thread.new { run }
        @thread.report_on_exception = false
      end
    end

    def schedule_frames(frames)
      encoded = frames.map { |frame| encoded_frame(frame) }
      schedule(frame_channel(encoded.first), segments_for(encoded))
    end

    def schedule_data(data)
      frames = split_frames(data)
      channel = frame_channel(frames.first)
      messages = [[]]

      frames.each do |frame|
        if frame_type(frame) == METHOD_FRAME && messages.last.any?
          messages << []
        end
        messages.last << frame
      end

      messages.each do |message_frames|
        next if message_frames.empty?

        schedule(channel, segments_for(message_frames))
      end
    end

    def schedule_frame(frame)
      data = encoded_frame(frame)
      schedule(frame_channel(data), [data], true)
    end

    def abort(error = ConnectionClosedError.new("connection was closed while frames were pending"),
              stop_thread: true)
      stop(kill_thread: stop_thread)
      fail_jobs(error)
    end

    private

    attr_reader :queues, :active_channel, :sent

    def run
      loop do
        job = next_job
        break unless job

        drain_job(job)
      end
    end

    def next_job
      @mutex.synchronize do
        @condition.wait_while { @running && empty? }
        return nil unless @running

        channel = next_channel
        return nil unless channel

        @active_channel = channel
        @queues[channel].first
      end
    end

    def next_channel
      control_channel = @round.find do |channel|
        channel != @active_channel && @queues[channel].first.control
      end
      return control_channel if control_channel

      eligible = @round.select do |channel|
        job = @queues[channel].first
        channel != @active_channel && !job.control && !exceeds_budget?(@sent[channel], job.segments)
      end

      if eligible.empty?
        active = @round.reject { |channel| channel == @active_channel || @queues[channel].first.control }
        if active.any? && active.all? { |channel| exceeds_budget?(@sent[channel], @queues[channel].first.segments) }
          active.each { |channel| @sent[channel] = 0 }
          eligible = active
        end
      end

      eligible.min_by { |channel| [@sent[channel], @round.index(channel)] }
    end

    def exceeds_budget?(sent, segments)
      next_body = segments.find { |segment| frame_type(segment) == BODY_FRAME }
      next_body && sent + next_body.bytesize > @byte_budget
    end

    def drain_job(job)
      started = false
      until job.segments.empty?
        segment = job.segments.first
        written = @write.call(segment)
        unless written && written.positive?
          job.fail(NetworkFailure.new("detected a network failure while writing frames", nil))
          abort(stop_thread: false)
          return
        end
        started = true

        @mutex.synchronize do
          unless @running
            job.fail(ConnectionClosedError.new("connection changed before frames were fully written"))
            return
          end
          job.segments.shift
          @sent[job.channel] += segment.bytesize if frame_type(segment) == BODY_FRAME
          @active_channel = nil if frame_type(segment) == BODY_FRAME && job.segments.any?
        end

        return if @active_channel.nil? && job.segments.any?
      end

      finish_job(job)
    rescue StandardError
      job.fail(ConnectionClosedError.new("connection failed while frames were pending")) if started
      abort(stop_thread: false)
    end

    def schedule(channel, segments, control = false)
      raise ConnectionClosedError.new(segments) if channel.zero? && !control

      job = Job.new(channel, segments, control)
      @mutex.synchronize do
        raise ConnectionClosedError.new(segments) unless @running

        @queues[channel] << job
        @round << channel unless @round.include?(channel)
        @condition.signal
      end

      job.wait
    end

    def empty?
      @queues.values.all?(&:empty?)
    end

    def finish_job(job)
      @mutex.synchronize do
        queue = @queues[job.channel]
        queue.shift if @active_channel == job.channel && queue.first == job
        @active_channel = nil

        if queue.empty?
          @queues.delete(job.channel)
          @round.delete(job.channel)
          @sent.delete(job.channel)
        end
      end
      job.complete
    end

    def fail_jobs(error)
      jobs = @mutex.synchronize do
        jobs = @queues.values.flatten
        @queues.clear
        @round.clear
        @sent.clear
        jobs
      end
      jobs.each { |job| job.fail(error) }
    end

    def stop(kill_thread: true)
      thread = @mutex.synchronize do
        return false unless @running

        @running = false
        @condition.broadcast
        @thread
      end
      thread.exit if kill_thread && thread != Thread.current
      true
    end

    def segments_for(frames)
      segments = []
      prefix = +""

      frames.each do |frame|
        case frame_type(frame)
        when METHOD_FRAME, HEADER_FRAME
          prefix << frame
        else
          unless prefix.empty?
            segments << prefix
            prefix = +""
          end
          segments << frame
        end
      end

      segments << prefix unless prefix.empty?
      segments
    end

    def split_frames(data)
      frames = []
      offset = 0

      while offset < data.bytesize
        raise ArgumentError, "incomplete AMQP frame" if offset + 7 > data.bytesize

        payload_size = data.byteslice(offset + 3, 4).unpack1("N")
        length = 7 + payload_size + 1
        raise ArgumentError, "incomplete AMQP frame" if offset + length > data.bytesize

        frames << data.byteslice(offset, length)
        offset += length
      end

      frames
    end

    def encoded_frame(frame)
      frame.respond_to?(:encode) ? frame.encode : frame
    end

    def frame_type(frame)
      frame.getbyte(0)
    end

    def frame_channel(frame)
      frame.byteslice(1, 2).unpack1("n")
    end
  end
end
