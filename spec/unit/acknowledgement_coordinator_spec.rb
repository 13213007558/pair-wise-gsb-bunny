# frozen_string_literal: true
require "spec_helper"

describe Bunny::AcknowledgementCoordinator do
  # Records the ack/nack frames the coordinator emits
  let(:channel) do
    Class.new do
      attr_reader :acks, :nacks

      def initialize
        @acks  = []
        @nacks = []
      end

      def basic_ack(delivery_tag, multiple)
        @acks << [delivery_tag, multiple]
      end

      def basic_nack(delivery_tag, multiple, requeue)
        @nacks << [delivery_tag, multiple, requeue]
      end
    end.new
  end
  let(:coordinator) { described_class.new(channel) }

  def credential(tag)
    coordinator.credential_for(tag)
  end

  describe "out-of-order completion" do
    before { [10, 11, 12].each { |t| coordinator.track(t) } }

    it "emits nothing while earlier deliveries are still running" do
      credential(12).complete

      expect(channel.acks).to be_empty
      expect(channel.nacks).to be_empty
      expect(coordinator.pending_tags).to eq([10, 11])
    end

    it "emits cumulative acks only across the contiguous resolved frontier" do
      credential(12).complete
      credential(10).complete
      expect(channel.acks).to eq([[10, true]])

      credential(11).complete
      expect(channel.acks).to eq([[10, true], [12, true]])
      expect(coordinator.pending_tags).to be_empty
    end

    it "nacks rejected deliveries individually and acks across the hole" do
      credential(12).complete
      credential(10).reject
      expect(channel.nacks).to eq([[10, false, false]])
      expect(channel.acks).to be_empty

      credential(11).complete
      expect(channel.acks).to eq([[12, true]])
      expect(coordinator.pending_tags).to be_empty
    end

    it "requeues via an individual nack with requeue set" do
      credential(11).requeue

      expect(channel.nacks).to eq([[11, false, true]])
      expect(channel.acks).to be_empty
      expect(coordinator.pending_tags).to eq([10, 12])
    end
  end

  describe "result submission validation" do
    before { coordinator.track(10) }

    it "accepts a delivery info object when issuing a credential" do
      delivery_info = instance_double(Bunny::DeliveryInfo, delivery_tag: 10)

      expect(coordinator.credential_for(delivery_info).delivery_tag).to eq(10)
    end

    it "rejects results for tags that were never tracked" do
      expect { credential(99).complete }.to raise_error(described_class::UnknownDeliveryError)
    end

    it "rejects resolving the same delivery twice" do
      credential(10).complete

      expect { credential(10).complete }.to raise_error(described_class::AlreadyResolvedError)
    end
  end

  describe "channel recovery" do
    it "rejects pre-recovery credentials and keeps new deliveries pending" do
      [10, 11, 12].each { |t| coordinator.track(t) }
      c10 = credential(10)
      c11 = credential(11)
      c12 = credential(12)

      c12.complete
      c10.complete
      expect(channel.acks).to eq([[10, true]])

      # the connection is recovered before tag 11 finishes processing
      coordinator.reset!

      expect { c11.complete }.to raise_error(described_class::StaleCredentialError)
      expect { c12.complete }.to raise_error(described_class::StaleCredentialError)
      expect { c10.reject }.to raise_error(described_class::StaleCredentialError)

      # the recovered channel restarts the delivery tag space
      coordinator.track(10)
      coordinator.track(11)
      expect(coordinator.pending_tags).to eq([10, 11])

      # a stale credential must not confirm the new incarnation's identical tag
      expect { c10.complete }.to raise_error(described_class::StaleCredentialError)
      expect(coordinator.pending_tags).to eq([10, 11])
      expect(channel.acks).to eq([[10, true]])

      coordinator.credential_for(10).complete
      expect(channel.acks).to eq([[10, true], [10, true]])
      expect(coordinator.pending_tags).to eq([11])
    end
  end

  describe "channel integration" do
    let(:logger)  { Logger.new(IO::NULL) }
    let(:session) { Bunny.new(host: "127.0.0.1", port: 65_534, logger: logger) }
    let(:channel) { Bunny::Channel.new(session, 1) }

    def deliver(tag)
      channel.handle_frameset(AMQ::Protocol::Basic::Deliver.new("ctag", tag, false, "amq.topic", "rk"), nil, nil)
    end

    def deliver_to(consumer_tag, tag)
      channel.handle_frameset(AMQ::Protocol::Basic::Deliver.new(consumer_tag, tag, false, "amq.topic", "rk"), nil, nil)
    end

    it "is created lazily and memoized per channel" do
      coordinator = channel.acknowledgement_coordinator

      expect(coordinator).to be_a(Bunny::AcknowledgementCoordinator)
      expect(channel.acknowledgement_coordinator).to equal(coordinator)
    end

    it "tracks deliveries from multiple consumers in the channel delivery tag space" do
      coordinator = channel.acknowledgement_coordinator

      deliver_to("consumer-a", 1)
      deliver_to("consumer-b", 2)
      deliver_to("consumer-a", 3)

      expect(coordinator.pending_tags).to eq([1, 2, 3])
    end

    it "tracks deliveries and invalidates credentials on recovery" do
      coordinator = channel.acknowledgement_coordinator

      deliver(1)
      deliver(2)
      expect(coordinator.pending_tags).to eq([1, 2])

      stale = coordinator.credential_for(1)

      channel.connection_closed!
      channel.recover_from_network_failure

      expect { stale.complete }.to raise_error(described_class::StaleCredentialError)
      expect(coordinator.pending_tags).to be_empty

      deliver(1)
      expect(coordinator.pending_tags).to eq([1])
    end
  end
end
