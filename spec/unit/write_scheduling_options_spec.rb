# frozen_string_literal: true
require "spec_helper"

describe Bunny::Session, "cross-channel write scheduling options" do
  let(:logger) { Logger.new(IO::NULL) }

  def new_session(opts = {})
    Bunny::Session.new({ logger: logger }.merge(opts))
  end

  it "keeps cross-channel scheduling disabled by default" do
    session = new_session

    expect(session.cross_channel_write_scheduling?).to be(false)
    expect(session.write_scheduler_byte_budget).to eq(16384)
  end

  it "rejects byte budgets that cannot contain an AMQP frame header and trailer" do
    expect { new_session(cross_channel_write_scheduling: true, write_scheduler_byte_budget: 8) }
      .to raise_error(ArgumentError, /greater than 8/)
  end

  it "caps publishing frame size at the scheduling byte budget" do
    session = new_session(cross_channel_write_scheduling: true,
                          write_scheduler_byte_budget: 128)
    session.instance_variable_set(:@frame_max, 131_072)

    expect(session.publish_frame_max).to eq(128)
  end
end
