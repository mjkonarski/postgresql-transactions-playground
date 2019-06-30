require 'pg'
require 'ap'
load 'test_case.rb'
load 'runner.rb'

class Sample < TestCase
    def prepare
        pg_conn.exec( "SELECT * FROM pg_stat_activity;" ) do |result|
            result.each do |row|
                ap row.values_at('procpid', 'usename', 'current_query')
            end
        end
    end

    def work(worker_num:)
        logger.info("I'm the worker #{worker_num}")
    end

end

Runner.new.run(test_case: Sample.new, workers_num: 5)