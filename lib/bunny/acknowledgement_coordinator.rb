# frozen_string_literal: true

module Bunny
  # Coordinates consumer delivery acknowledgements on a single channel when
  # deliveries are processed concurrently (e.g. dispatched to a thread pool)
  # and therefore complete out of order.
  #
  # AMQP 0.9.1 delivery tags are scoped to a channel, so one coordinator
  # covers every consumer on its channel. The coordinator tracks every
  # delivery on the channel and only emits a cumulative +basic.ack+ across
  # the contiguous frontier of resolved deliveries. Deliveries rejected or
  # requeued individually become "holes" that are nacked one by one, while
  # deliveries still being processed keep the frontier from advancing.
  #
  # Results are submitted via credentials obtained from the coordinator.
  # A credential is bound to the channel incarnation (session) it was
  # issued on: after automatic network failure recovery the broker resets
  # the delivery tag space, so credentials issued before recovery are
  # rejected with {StaleCredentialError} and can never confirm a delivery
  # on the recovered channel that happens to carry the same tag.
  #
  # When a coordinator is used on a channel, route every acknowledgement
  # on that channel through it. Mixing it with direct
  # {Bunny::Channel#ack}, {Bunny::Channel#nack} or manual +basic.get+
  # acknowledgements on the same channel defeats frontier tracking.
  #
  # @example
  #   coordinator = ch.acknowledgement_coordinator
  #   q.subscribe(manual_ack: true) do |delivery_info, _properties, payload|
  #     credential = coordinator.credential_for(delivery_info)
  #     executor.submit do
  #       process(payload)
  #       credential.complete
  #     rescue TransientError
  #       credential.requeue
  #     rescue StandardError
  #       credential.reject
  #     end
  #   end
  class AcknowledgementCoordinator
    # Raised when a credential issued before a channel recovery (or close)
    # is used to submit a result afterwards
    class StaleCredentialError < StandardError; end
    # Raised when a result is submitted for a delivery tag the coordinator
    # has never seen on the current channel incarnation
    class UnknownDeliveryError < StandardError; end
    # Raised when a result is submitted for a delivery that was already resolved
    class AlreadyResolvedError < StandardError; end

    # A handle for submitting the outcome of a single delivery. Bound to
    # the channel incarnation it was issued on.
    class Credential
      # @return [Integer] Delivery tag this credential refers to
      attr_reader :delivery_tag
      # @return [Integer] Channel incarnation this credential belongs to
      # @private
      attr_reader :epoch

      # @private
      def initialize(coordinator, delivery_tag, epoch)
        @coordinator  = coordinator
        @delivery_tag = delivery_tag
        @epoch        = epoch
      end

      # Acknowledges the delivery once every earlier delivery on the
      # channel is resolved as well
      def complete
        @coordinator.complete(self)
      end

      # Rejects the delivery without requeueing
      def reject
        @coordinator.reject(self)
      end

      # Rejects the delivery, asking the broker to requeue it
      def requeue
        @coordinator.requeue(self)
      end
    end

    # @param [Bunny::Channel] channel Channel this coordinator tracks deliveries for
    def initialize(channel)
      @channel = channel
      @mutex   = ::Mutex.new

      @epoch     = 0
      @pending   = {}
      @resolved  = {}
      @frontier  = nil
    end

    # Returns a credential for a freshly received delivery.
    #
    # @param [Bunny::DeliveryInfo, Integer] delivery_info_or_tag Delivery or its tag
    # @return [Bunny::AcknowledgementCoordinator::Credential]
    def credential_for(delivery_info_or_tag)
      tag = delivery_info_or_tag.respond_to?(:delivery_tag) ? delivery_info_or_tag.delivery_tag : delivery_info_or_tag
      Credential.new(self, tag.to_i, current_epoch)
    end

    # Submits a successful outcome for the delivery. The cumulative ack is
    # only emitted once all earlier deliveries are resolved.
    #
    # @param [Bunny::AcknowledgementCoordinator::Credential] credential
    def complete(credential)
      resolve(credential, :ack)
    end

    # Submits a rejection without requeueing. Emitted as an individual
    # +basic.nack+ immediately, leaving a resolved "hole" the cumulative
    # ack frontier can later move across.
    #
    # @param [Bunny::AcknowledgementCoordinator::Credential] credential
    def reject(credential)
      resolve(credential, :nack, false)
    end

    # Submits a rejection with requeueing.
    #
    # @param [Bunny::AcknowledgementCoordinator::Credential] credential
    def requeue(credential)
      resolve(credential, :nack, true)
    end

    # @return [Array<Integer>] Delivery tags still awaiting a result, in order
    def pending_tags
      @mutex.synchronize { @pending.keys.sort }
    end

    # Records a delivery on the channel. Called by {Bunny::Channel} for
    # every +basic.deliver+ before it is dispatched to a consumer.
    # @private
    def track(delivery_tag)
      tag = delivery_tag.to_i
      @mutex.synchronize do
        @frontier = tag - 1 if @frontier.nil?
        @pending[tag] = true
      end
      nil
    end

    # Invalidates all outstanding credentials and forgets all deliveries.
    # Called by {Bunny::Channel} when the connection is lost and when the
    # channel is recovered, after which the broker restarts the delivery
    # tag space.
    # @private
    def reset!
      @mutex.synchronize do
        @epoch += 1
        @pending.clear
        @resolved.clear
        @frontier = nil
      end
      nil
    end

    private

    def current_epoch
      @mutex.synchronize { @epoch }
    end

    def resolve(credential, action, requeue = false)
      tag = credential.delivery_tag
      @mutex.synchronize do
        unless credential.epoch == @epoch
          raise StaleCredentialError,
                "credential for delivery tag #{tag} was issued before the channel was recovered"
        end
        if !@pending.include?(tag)
          if @resolved.key?(tag) || (@frontier && tag <= @frontier)
            raise AlreadyResolvedError, "delivery tag #{tag} was already resolved"
          else
            raise UnknownDeliveryError, "delivery tag #{tag} was never tracked on this channel"
          end
        end

        case action
        when :ack
          @pending.delete(tag)
          @resolved[tag] = :acked
          advance_frontier
        when :nack
          @channel.basic_nack(tag, false, requeue)
          @pending.delete(tag)
          @resolved[tag] = :nacked
          advance_frontier
        end
      end
      nil
    end

    # Moves the frontier across the contiguous run of resolved deliveries
    # and emits a single cumulative ack up to the highest acknowledged tag
    # in that run. Individually nacked holes below it are already settled
    # and ignored by the broker for a multiple ack.
    def advance_frontier
      return if @frontier.nil?

      tag           = @frontier
      highest_acked = nil
      while (state = @resolved[tag + 1])
        tag += 1
        highest_acked = tag if state == :acked
      end
      return if tag == @frontier

      @channel.basic_ack(highest_acked, true) if highest_acked
      @resolved.delete_if { |t, _| t <= tag }
      @frontier = tag
    end
  end
end
