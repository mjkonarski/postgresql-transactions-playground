require 'pg'
require 'ap'
require 'retryable'
load 'test_case.rb'
load 'runner.rb'

ROOM_NUMBER = 1
WORKERS_NUM = 2
MIN_TIMESTAMP = DateTime.parse('2019-06-01 00:00').to_time
MAX_TIMESTAMP = DateTime.parse('2019-07-30 00:00').to_time
MAX_HOURS = 5

#
# Fibbonaci 
#

class Rooms < TestCase
    def prepare
        sql = <<SQL
        DROP TABLE IF EXISTS rooms;
        create table rooms  (
            id numeric,
            start_time timestamp without time zone,
            end_time timestamp without time zone,
            CHECK ( start_time < end_time )
        );
SQL
        pg_conn.exec(sql)
    end

    def work(worker_num:)
        loop do
            exception_cb = proc do |exception|
                logger.debug("#{worker_num}: #{exception}")
                pg_conn.exec("rollback")
            end

            Retryable.retryable(tries: :infinite, exception_cb: exception_cb) do |retry_num|
                pg_conn.exec("begin transaction isolation level serializable")
                res = pg_conn.exec("select id, start_time, end_time from rooms "\
                    "where start_time >= '#{db_timestamp(MIN_TIMESTAMP)}' and end_time <= '#{db_timestamp(MAX_TIMESTAMP)}' "\
                    "and id = #{ROOM_NUMBER} order by start_time")
            
                left = MIN_TIMESTAMP    

                empty_ranges = []

                res.each do |row|
                    start_time = DateTime.parse(row['start_time']).to_time
                    end_time = DateTime.parse(row['end_time']).to_time
                    
                    empty_ranges << [left, start_time] if left < start_time

                    left = end_time
                end
                empty_ranges << [left, MAX_TIMESTAMP] if left < MAX_TIMESTAMP

                if empty_ranges.empty?
                    logger.info("#{worker_num}: no more space")
                    pg_conn.exec("commit")
                    return 
                end

                random_range = empty_ranges.sample
                start_time = random_range[0]
                end_time = random_range[1]
                range_hours = (end_time - start_time).to_i / 3600
                new_range_hours = rand(1..[range_hours, MAX_HOURS].min)
                new_range_offset = rand(0..(range_hours - new_range_hours))

                new_start_time = start_time + new_range_offset * 3600
                new_end_time = new_start_time + new_range_hours * 3600
                
                res = pg_conn.exec("insert into rooms(id, start_time, end_time) "\
                    "values(#{ROOM_NUMBER}, '#{db_timestamp(new_start_time)}', '#{db_timestamp(new_end_time)}')")
                pg_conn.exec("commit")
            end
            logger.debug("#{worker_num} committed")

        end
    end

    def validate
        res = pg_conn.exec("select id, start_time, end_time from rooms order by id, start_time")
       
        rooms = []
       
        left = MIN_TIMESTAMP
        correct = true

        res.each do |row|
            start_time = DateTime.parse(row['start_time']).to_time
            end_time = DateTime.parse(row['end_time']).to_time

            correct = false if left > start_time || start_time > end_time 
            left = end_time
            rooms << [start_time, end_time]
        end

        ap rooms
        logger.info("The result is: #{correct}")
    end

    private

    def db_timestamp(t)
        t.strftime('%Y-%m-%d %H:%M:%S')
    end

end

Runner.new.run(test_case: Rooms.new, workers_num: WORKERS_NUM)