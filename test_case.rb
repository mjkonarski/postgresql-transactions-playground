require 'logger'
require 'pg'

class TestCase
    def initialize
        @logger = Logger.new(STDOUT)
        @pg_conn = PG.connect(ENV['PG_CONNECTION_STRING'])
    end

    def prepare; end
    def work(worker_num:); end
    def validate; end
    def cleanup; end

    private 
    
    attr_reader :logger, :pg_conn
end