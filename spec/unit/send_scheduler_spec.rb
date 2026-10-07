# frozen_string_literal: true
require "spec_helper"

describe Bunny::SendScheduler do
  def publish_options
    { delivery_mode: 1, content_type: "application/octet-stream", priority: 0 }
  end

  def frames(channel, payload, frame_max = 40)
    AMQ::Protocol::Basic::Publish.encode(channel, payload, publish_options, "", "", false, false, frame_max)
  end

  def parse(data)
    parsed = []
    offset = 0

    while offset < data.bytesize
      type, channel, size = data.byteslice(offset, 7).unpack("CnN")
      payload = data.byteslice(offset + 7, size)
      expect(data.getbyte(offset + 7 + size)).to eq(AMQ::Protocol::Frame::FINAL_OCTET.ord)
      parsed << [type, channel, payload]
      offset += 7 + size + 1
    end

    parsed
  end

  it "reassembles every frameset after one-byte partial writes" do
    mutex = Mutex.new
    written = +""
    fake = Object.new
    fake.define_singleton_method(:write_some) do |data, offset, _limit|
      mutex.synchronize do
        written << data.byteslice(offset, 1)
        1
      end
    end

    scheduler = described_class.new(-> { fake }, 32)
    scheduler.start

    large_payload = "A" * 56
    second_payload = "B" * 40
    small_payload = "small"

    threads = [
      Thread.new { scheduler.send_frames(frames(1, large_payload)) },
      Thread.new { scheduler.send_frames(frames(3, second_payload)) },
      Thread.new { scheduler.send_frames(frames(2, small_payload)) }
    ]

    deadline = Time.now + 5
    sleep 0.001 until written.bytesize == 339 || Time.now > deadline
    expect(written.bytesize).to eq(339)
    threads.each { |thread| thread.join(1) }

    wire_frames = parse(written)
    bodies = wire_frames.each_with_object(Hash.new { |hash, key| hash[key] = +"" }) do |(type, channel, payload), result|
      result[channel] << payload if type == 3
    end

    expect(bodies[1]).to eq(large_payload)
    expect(bodies[2]).to eq(small_payload)
    expect(bodies[3]).to eq(second_payload)
    expect(wire_frames.count { |type, _, _| type == 3 }).to eq(5)

    scheduler.stop
  end

  it "fairly selects the channel with fewer body bytes in a round" do
    scheduler = described_class.new(nil, 32)
    body_frame = AMQ::Protocol::BodyFrame.new("", 1).encode
    large = scheduler.send(:build_job, 1, [body_frame], false, false)
    small = scheduler.send(:build_job, 2, [body_frame.dup.tap { |frame| frame.setbyte(2, 2) }], false, false)

    scheduler.instance_variable_get(:@order).push(1, 2)
    scheduler.instance_variable_get(:@queues)[1] << large
    scheduler.instance_variable_get(:@queues)[2] << small
    scheduler.instance_variable_get(:@body_bytes_written)[1] = 32
    scheduler.instance_variable_get(:@exhausted_body_channels) << 1

    selected = scheduler.send(:select_job)
    expect(scheduler.instance_variable_get(:@byte_budget)).to eq(32)
    expect(scheduler.instance_variable_get(:@body_bytes_written)[1]).to eq(32)
    expect(scheduler.send(:body_fragment?, large.fragments.first)).to eq(true)
    expect(scheduler.send(:writable?, large)).to eq(false)
    expect(selected.channel).to eq(2)
    expect(selected).to equal(small)
  end

  it "schedules channel 0 control frames before queued content" do
    scheduler = described_class.new(nil, 32)
    content_frame = AMQ::Protocol::BodyFrame.new("payload", 1).encode
    heartbeat = AMQ::Protocol::HeartbeatFrame.encode
    content = scheduler.send(:build_job, 1, [content_frame], false, false)
    control = scheduler.send(:build_job, 0, [heartbeat], true, false)

    scheduler.instance_variable_get(:@order).push(1, 0)
    scheduler.instance_variable_get(:@queues)[1] << content
    scheduler.instance_variable_get(:@queues)[0] << control

    expect(scheduler.send(:select_job)).to equal(control)
  end

  it "fails queued jobs when the transport generation is reset" do
    scheduler = described_class.new(nil, 32)
    frame = AMQ::Protocol::BodyFrame.new("payload", 1).encode
    job = scheduler.send(:build_job, 1, [frame], false, false)
    scheduler.instance_variable_get(:@queues)[1] << job
    scheduler.instance_variable_get(:@order) << 1

    scheduler.reset

    expect(job.complete?).to eq(true)
    expect(job.error).to be_a(Bunny::ConnectionClosedError)
    expect(scheduler.instance_variable_get(:@queues)).to be_empty
  end

  it "limits a body frame write to the channel's remaining byte budget" do
    limits = []
    fake = Object.new
    fake.define_singleton_method(:write_some) do |_frame, _offset, limit|
      limits << limit
      limit
    end

    scheduler = described_class.new(-> { fake }, 32)
    frame = AMQ::Protocol::BodyFrame.new("x" * 24, 1).encode
    job = scheduler.send(:build_job, 1, [frame], false, false)
    scheduler.instance_variable_get(:@queues)[1] << job
    scheduler.instance_variable_get(:@order) << 1

    selected = scheduler.send(:select_job)
    scheduler.send(:write_job, selected)

    expect(limits).to eq([32])
  end
end
