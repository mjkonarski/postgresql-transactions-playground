require 'logger'
require 'pg'

class TestCase
    def initialize
        loglevels = %w[DEBUG INFO WARN ERROR FATAL UNKNOWN].freeze
        @logger = Logger.new(STDOUT)
        @logger.level = loglevels.index(ENV.fetch("LOG_LEVEL","WARN")) || Logger::WARN

        @pg_conn = PG.connect(ENV['PG_CONNECTION_STRING'])
    end

    def prepare; end
    def work(worker_num:); end
    def validate; end
    def cleanup; end

    private 
    
    attr_reader :logger, :pg_conn
end