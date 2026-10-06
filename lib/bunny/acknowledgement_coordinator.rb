# frozen_string_literal: true

require "set"

require "bunny/exceptions"

module Bunny
  # Coordinates consumer acknowledgements on a single channel so that
  # deliveries processed concurrently (e.g. on a multi-threaded consumer
  # work pool) can be acknowledged with batched (multiple: true) acks
  # without ever acknowledging a delivery that is still being processed.
  #
  # Delivery tags are scoped to a channel and are monotonic within it,
  # so a single coordinator covers all consumers that share the channel.
  # The coordinator tracks every registered delivery and only advances
  # its acknowledgement frontier across a contiguous run of resolved
  # deliveries. Deliveries that are rejected or requeued are nacked
  # individually right away and recorded as holes the frontier can
  # later cross.
  #
  # Each registered delivery yields a {Credential} that is bound to the
  # current channel session. After an automatic connection recovery the
  # channel is reopened and its delivery tag sequence restarts, so
  # credentials issued before the recovery are rejected with
  # {Bunny::StaleDeliveryCredential} instead of acknowledging an
  # unrelated delivery that happens to carry the same tag.
  #
  # @example
  #   coordinator = ch.acknowledgement_coordinator
  #
  #   q.subscribe(manual_ack: true) do |delivery, _properties, _body|
  #     credential = coordinator.track(delivery.delivery_tag)
  #     pool.post do
  #       begin
  #         process(delivery)
  #         credential.ack
  #       rescue TransientError
  #         credential.requeue
  #       rescue
  #         credential.reject
  #       end
  #     end
  #   end
  #
  # Once a coordinator exists for a channel, deliveries dispatched to
  # consumers on that channel are tracked automatically in delivery
  # order; calling {#track} for such a delivery simply returns its
  # credential. Obtain the coordinator before starting consumers so
  # that no delivery goes untracked.
  class AcknowledgementCoordinator
    # A handle for a single tracked delivery, bound to the channel
    # session (epoch) it was issued in. Resolution methods return true
    # when the call resolved the delivery and false when the delivery
    # had already been resolved.
    class Credential
      # @return [Integer] Delivery tag on the channel
      attr_reader :delivery_tag
      # @return [Integer] Channel session this credential belongs to
      attr_reader :epoch

      # @private
      def initialize(coordinator, delivery_tag, epoch)
        @coordinator  = coordinator
        @delivery_tag = delivery_tag
        @epoch        = epoch
      end

      # Acknowledges the delivery. The actual basic.ack frame may be
      # deferred and batched until earlier deliveries are resolved.
      def ack
        @coordinator.ack(self)
      end

      # Rejects the delivery without requeueing it (drops it or routes
      # it to a dead-letter exchange). Emits an individual basic.nack.
      def reject
        @coordinator.reject(self)
      end

      # Rejects the delivery and asks RabbitMQ to requeue it. Emits an
      # individual basic.nack with requeue set.
      def requeue
        @coordinator.requeue(self)
      end
    end

    # @return [Bunny::Channel] Channel this coordinator tracks deliveries for
    attr_reader :channel

    def initialize(channel)
      @channel = channel
      @mutex   = (channel.connection ? channel.connection.mutex_impl : ::Mutex).new

      reset_state(channel.acknowledgement_epoch)
    end

    # Registers a delivery tag and returns a credential for it. Called
    # automatically for every consumer delivery once the coordinator
    # exists; safe to call again for an already tracked (but not yet
    # resolved) delivery.
    #
    # @param [Integer] delivery_tag Delivery tag to track
    # @return [Bunny::AcknowledgementCoordinator::Credential]
    def track(delivery_tag)
      tag = delivery_tag.to_i

      @mutex.synchronize do
        sync_epoch!

        if @pending.include?(tag)
          # already tracked, hand out another credential for it
        elsif tag <= @frontier || @resolved_acks.include?(tag) || @holes.include?(tag)
          raise ArgumentError, "delivery tag #{tag} was already resolved on channel #{@channel.id}"
        else
          @pending << tag
          @max_tracked_tag = tag if tag > @max_tracked_tag
        end

        Credential.new(self, tag, @epoch)
      end
    end

    # Resolves a tracked delivery as successfully processed.
    # @return [Boolean] true if this call resolved the delivery
    def ack(credential)
      resolve(credential, :ack, false)
    end

    # Resolves a tracked delivery as rejected without requeueing.
    # @return [Boolean] true if this call resolved the delivery
    def reject(credential)
      resolve(credential, :nack, false)
    end

    # Resolves a tracked delivery as rejected with requeueing.
    # @return [Boolean] true if this call resolved the delivery
    def requeue(credential)
      resolve(credential, :nack, true)
    end

    # @return [Boolean] true if the delivery tag is tracked and not yet resolved
    def pending?(delivery_tag)
      @mutex.synchronize do
        sync_epoch!
        @pending.include?(delivery_tag.to_i)
      end
    end

    # @return [Integer] Number of tracked deliveries not yet resolved
    def pending_count
      @mutex.synchronize do
        sync_epoch!
        @pending.size
      end
    end

    # @return [Integer] Highest delivery tag such that it and all tracked
    #   deliveries before it are resolved
    def frontier
      @mutex.synchronize do
        sync_epoch!
        @frontier
      end
    end

    # @return [Integer] Channel session (epoch) this coordinator currently tracks
    def epoch
      @mutex.synchronize do
        sync_epoch!
        @epoch
      end
    end

    private

    def resolve(credential, outcome, requeue)
      @mutex.synchronize do
        sync_epoch!

        unless credential.epoch == @epoch
          message = "delivery tag #{credential.delivery_tag} was issued for a previous session of " +
            "channel #{@channel.id} (epoch #{credential.epoch}, current #{@epoch}); " +
            "it must not be resolved after a connection recovery"
          raise StaleDeliveryCredential, message
        end

        tag = credential.delivery_tag
        return false unless @pending.delete?(tag)

        if outcome == :ack
          @resolved_acks << tag
        else
          # nack individually right away: the delivery is done and the
          # broker can drop or requeue it without waiting for neighbours
          @holes << tag
          @channel.basic_nack(tag, false, requeue)
        end

        advance_frontier

        true
      end
    end

    # Advances the frontier over the contiguous run of resolved
    # deliveries and emits a single batched basic.ack for the acked
    # ones. Individually nacked holes are already settled with the
    # broker, so a multiple: true ack up to the frontier only covers
    # deliveries resolved as acked.
    def advance_frontier
      target = if @pending.empty?
                 @max_tracked_tag
               else
                 @pending.min - 1
               end
      return if target <= @frontier

      batch = @resolved_acks.select { |tag| tag <= target }
      @channel.basic_ack(batch.max, true) unless batch.empty?
      @resolved_acks.subtract(batch)
      @holes.delete_if { |tag| tag <= target }

      @frontier = target
    end

    def sync_epoch!
      current = @channel.acknowledgement_epoch
      reset_state(current) if current != @epoch
    end

    def reset_state(epoch)
      @epoch           = epoch
      @frontier        = 0
      @max_tracked_tag = 0
      @pending         = Set.new
      @resolved_acks   = Set.new
      @holes           = Set.new
    end
  end
end
