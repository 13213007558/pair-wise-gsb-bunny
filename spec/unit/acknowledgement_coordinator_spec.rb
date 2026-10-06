# -*- coding: utf-8 -*-
# frozen_string_literal: true
require "spec_helper"

describe Bunny::AcknowledgementCoordinator do
  # These examples exercise the acknowledgement coordination logic
  # without a running RabbitMQ node: frames emitted by the channel
  # are captured instead of being sent.

  let(:logger) { Logger.new(IO::NULL) }
  let(:session) do
    Bunny.new({
      host: "127.0.0.1",
      port: 65_534,
      logger: logger
    })
  end
  let(:channel) { Bunny::Channel.new(session, 1) }
  subject(:coordinator) { channel.acknowledgement_coordinator }

  let(:acks)  { [] }
  let(:nacks) { [] }

  before do
    allow(channel).to receive(:basic_ack) { |tag, multiple| acks << [tag, multiple] }
    allow(channel).to receive(:basic_nack) { |tag, multiple, requeue| nacks << [tag, multiple, requeue] }
  end

  def recover_channel(ch)
    allow(session).to receive(:open_channel)
    ch.open
  end

  describe "with deliveries completed in the order 12, 10, 11" do
    it "only emits acks across the resolved contiguous frontier and nacks holes individually" do
      c10 = coordinator.track(10)
      c11 = coordinator.track(11)
      c12 = coordinator.track(12)

      # 12 finishes first: nothing can be emitted, 10 and 11 are still running
      c12.ack
      expect(acks).to be_empty
      expect(nacks).to be_empty
      expect(coordinator.frontier).to eq(9)

      # 10 finishes: the frontier covers 10 only, 11 is still running
      c10.ack
      expect(acks).to eq([[10, true]])
      expect(coordinator.frontier).to eq(10)
      expect(coordinator).to be_pending(11)
      expect(coordinator).not_to be_pending(12)

      # 11 is requeued: nacked individually, then the frontier crosses
      # the hole and covers 12 with a batched ack
      c11.requeue
      expect(nacks).to eq([[11, false, true]])
      expect(acks).to eq([[10, true], [12, true]])
      expect(coordinator.frontier).to eq(12)
      expect(coordinator.pending_count).to eq(0)
    end
  end

  describe "connection recovery before a delivery is resolved" do
    it "rejects stale credentials and keeps new deliveries pending" do
      c10 = coordinator.track(10)
      c11 = coordinator.track(11)
      c12 = coordinator.track(12)

      c12.ack
      c10.ack
      expect(acks).to eq([[10, true]])

      recover_channel(channel)

      # the channel session has changed, so the coordinator starts over
      expect(channel.acknowledgement_epoch).to eq(1)
      expect(coordinator.epoch).to eq(1)

      # credentials from before the recovery must not confirm deliveries
      # on the recovered channel, even for tags that happen to match
      expect { c11.requeue }.to raise_error(Bunny::StaleDeliveryCredential)
      expect { c12.ack }.to raise_error(Bunny::StaleDeliveryCredential)
      expect(nacks).to be_empty
      expect(acks).to eq([[10, true]])

      # deliveries on the recovered channel are tracked from a clean slate
      n1 = coordinator.track(1)
      n2 = coordinator.track(2)
      expect(coordinator).to be_pending(1)
      expect(coordinator).to be_pending(2)
      expect(coordinator.frontier).to eq(0)

      n1.ack
      expect(acks).to eq([[10, true], [1, true]])
      n2.ack
      expect(acks).to eq([[10, true], [1, true], [2, true]])
    end
  end

  describe "multiple consumers sharing a channel" do
    it "coordinates deliveries in the channel-wide tag space" do
      # consumer A receives tags 10 and 12, consumer B receives tag 11
      a1 = coordinator.track(10)
      b1 = coordinator.track(11)
      a2 = coordinator.track(12)

      a2.ack
      b1.ack
      expect(acks).to be_empty

      a1.ack
      expect(acks).to eq([[12, true]])
    end
  end

  describe "#reject" do
    it "emits an individual nack without requeueing" do
      c10 = coordinator.track(10)
      c11 = coordinator.track(11)

      c11.reject
      expect(nacks).to eq([[11, false, false]])
      expect(acks).to be_empty

      c10.ack
      expect(acks).to eq([[10, true]])
    end
  end

  describe "resolving the same delivery twice" do
    it "resolves once and does not emit extra frames" do
      credential = coordinator.track(10)

      expect(credential.ack).to be(true)
      expect(acks).to eq([[10, true]])

      expect(credential.ack).to be(false)
      expect(credential.reject).to be(false)
      expect(acks).to eq([[10, true]])
      expect(nacks).to be_empty
    end
  end

  describe "#track" do
    it "is idempotent for a delivery that is still pending" do
      first  = coordinator.track(10)
      second = coordinator.track(10)

      expect(coordinator.pending_count).to eq(1)

      first.ack
      expect(second.ack).to be(false)
      expect(acks).to eq([[10, true]])
    end

    it "raises for a delivery tag that was already resolved" do
      coordinator.track(10).ack

      expect { coordinator.track(10) }.to raise_error(ArgumentError)
    end
  end

  describe "delivery dispatch integration" do
    it "tracks deliveries automatically once the coordinator exists" do
      coordinator

      delivered = Queue.new
      consumer = double("consumer")
      allow(consumer).to receive(:call) { delivered << true }
      channel.instance_variable_get(:@consumers)["tag1"] = consumer

      channel.work_pool.start
      begin
        deliver = AMQ::Protocol::Basic::Deliver.new("tag1", 10, false, "", "bunny.tests")
        channel.handle_frameset(deliver, nil, "")
        delivered.pop

        expect(coordinator).to be_pending(10)
      ensure
        channel.work_pool.shutdown
      end
    end

    it "does not track deliveries when no coordinator exists" do
      delivered = Queue.new
      consumer = double("consumer")
      allow(consumer).to receive(:call) { delivered << true }
      channel.instance_variable_get(:@consumers)["tag1"] = consumer

      channel.work_pool.start
      begin
        deliver = AMQ::Protocol::Basic::Deliver.new("tag1", 10, false, "", "bunny.tests")
        channel.handle_frameset(deliver, nil, "")
        delivered.pop

        expect(channel.instance_variable_get(:@acknowledgement_coordinator)).to be_nil
      ensure
        channel.work_pool.shutdown
      end
    end
  end
end
