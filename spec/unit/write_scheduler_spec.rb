# frozen_string_literal: true
require "spec_helper"

describe Bunny::WriteScheduler do
  let(:budget) { 32 }

  def frames(channel, payload)
    AMQ::Protocol::Basic::Publish.encode(channel,
                                        payload,
                                        { content_type: "application/octet-stream" },
                                        "",
                                        "",
                                        false,
                                        false,
                                        budget - 8)
  end

  def reassemble(wire)
    messages = Hash.new { |hash, channel| hash[channel] = +"" }
    offset = 0

    while offset < wire.bytesize
      type, channel, size = wire.byteslice(offset, 7).unpack("CnN")
      payload = wire.byteslice(offset + 7, size)
      offset += 8 + size

      messages[channel] << payload if type == 3
    end

    messages
  end

  it "rotates queued body frames between channels using written wire bytes" do
    wire = +""
    mutex = Mutex.new
    gate = Queue.new
    release = Queue.new
    window = 7
    first_body_gated = false

    writer = proc do |segment|
      written = 0

      while written < segment.bytesize
        if segment.getbyte(0) == 3 && segment.byteslice(1, 2).unpack1("n") == 1 && !first_body_gated
          first_body_gated = true
          gate.push(:prefix)
          release.pop
        end

        count = [window, segment.bytesize - written].min
        mutex.synchronize { wire << segment.byteslice(written, count) }
        written += count
      end

      segment.bytesize
    end

    scheduler = described_class.new(writer, budget)
    scheduler.start

    large_payload_a = "A" * 160
    large_payload_b = "B" * 160
    small_payload = "s" * 16

    first = Thread.new { scheduler.schedule_frames(frames(1, large_payload_a)) }
    gate.pop

    second = Thread.new { scheduler.schedule_frames(frames(2, large_payload_b)) }
    small = Thread.new { scheduler.schedule_frames(frames(3, small_payload)) }

    sleep 0.01 until scheduler.send(:queues).keys.sort == [1, 2, 3]
    first_prefix_size = mutex.synchronize { wire.bytesize }
    release.push(:continue)

    [first, second, small].each(&:join)
    scheduler.abort

    messages = reassemble(wire)
    expect(messages[1]).to eq(large_payload_a)
    expect(messages[2]).to eq(large_payload_b)
    expect(messages[3]).to eq(small_payload)

    first_small_body = nil
    offset = 0
    while offset < wire.bytesize
      type, channel, size = wire.byteslice(offset, 7).unpack("CnN")
      first_small_body ||= offset if type == 3 && channel == 3
      offset += 8 + size
    end
    expect(first_small_body).not_to be_nil
    expect(first_small_body - first_prefix_size).to be < large_payload_a.bytesize + large_payload_b.bytesize
  end

  it "keeps rotating between two active channels after each body-frame budget" do
    order = Queue.new
    writer = proc do |segment|
      if segment.getbyte(0) == 3
        order.push(segment.byteslice(1, 2).unpack1("n"))
      end
      segment.bytesize
    end

    scheduler = described_class.new(writer, 24)
    scheduler.start

    threads = [
      Thread.new { scheduler.schedule_frames(frames(1, "A" * 48)) },
      Thread.new { scheduler.schedule_frames(frames(2, "B" * 48)) }
    ]

    frames_in_order = 4.times.map { order.pop }
    threads.each(&:join)
    scheduler.abort

    expect(frames_in_order).to eq([1, 2, 1, 2])
  end

  it "splits a raw batch back into complete message framesets" do
    wire = +""
    scheduler = described_class.new(->(segment) { wire << segment; segment.bytesize }, 4096)
    scheduler.start

    payload_a = "A" * 40
    payload_b = "B" * 24
    data = frames(1, payload_a).map(&:encode).join + frames(1, payload_b).map(&:encode).join
    scheduler.schedule_data(data)
    scheduler.abort

    expect(reassemble(wire)[1]).to eq(payload_a + payload_b)
  end

  it "sends an oversized body frame when one channel owns the whole round" do
    wire = +""
    scheduler = described_class.new(->(segment) { wire << segment; segment.bytesize }, 16)
    scheduler.start

    payload = "A" * 48
    scheduler.schedule_frames(frames(1, payload))
    scheduler.abort

    expect(reassemble(wire)[1]).to eq(payload)
  end

  it "discards unfinished frames when the transport session is replaced" do
    old_wire = +""
    new_wire = +""
    old_payload = "A" * 160
    new_payload = "s" * 16
    partial_written = false
    writer = proc do |segment|
      if segment.getbyte(0) == 3 && !partial_written
        partial_written = true
        old_wire << segment.byteslice(0, 5)
        raise Errno::EPIPE
      end
      old_wire << segment
      segment.bytesize
    end

    old_scheduler = described_class.new(writer, budget)
    new_scheduler = described_class.new(->(segment) { new_wire << segment; segment.bytesize }, budget)

    old_scheduler.start
    new_scheduler.start

    publishing = Thread.new { old_scheduler.schedule_frames(frames(1, old_payload)) }
    publishing.report_on_exception = false
    expect { publishing.join }.to raise_error(Bunny::ConnectionClosedError)

    new_scheduler.schedule_frames(frames(2, new_payload))
    new_scheduler.abort

    expect(old_wire.bytes).not_to include([3, 0, 2].pack("Cn"))
    expect(reassemble(new_wire)).to eq(2 => new_payload)
  end

  it "sends queued channel control frames before the next data body frame" do
    wire = +""
    gate = Queue.new
    release = Queue.new
    gated = false

    writer = proc do |segment|
      if segment.getbyte(0) == 3 && !gated
        gated = true
        gate.push(:body)
        release.pop
      end
      wire << segment
      segment.bytesize
    end

    scheduler = described_class.new(writer, budget)
    scheduler.start

    data = Thread.new { scheduler.schedule_frames(frames(1, "A" * 48)) }
    gate.pop

    control_frame = AMQ::Protocol::Channel::Close.encode(2, 200, "Goodbye", 0, 0)
    control = Thread.new { scheduler.schedule_frame(control_frame) }
    sleep 0.01 until scheduler.send(:queues).key?(2)
    release.push(:continue)

    [data, control].each(&:join)
    scheduler.abort

    first_control = wire.index(control_frame.encode)
    expect(first_control).not_to be_nil
    expect(first_control).to be < wire.bytesize - 24
  end
end
