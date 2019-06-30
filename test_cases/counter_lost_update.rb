require 'pg'
require 'ap'
load 'test_case.rb'
load 'runner.rb'

WORKERS_NUM = 2
NUMBER_OF_COUNTERS = 10
NUMBER_OF_INCREMENTS = 1000
# 
class AccountTranfersNonRepeatableRead < TestCase
    def prepare
        sql = <<SQL
        DROP TABLE IF EXISTS counters;
        create table counters(
            id SERIAL,
            value numeric
        );
        insert into counters(value) select 0 from generate_series(1, #{NUMBER_OF_COUNTERS});
SQL
        pg_conn.exec(sql)
    end

    def work(worker_num:)
        NUMBER_OF_INCREMENTS.times do            
            pg_conn.exec("begin")

            res = pg_conn.exec("select id, value from counters;")
            h = {}
            res.each do |row|
                h[row['id']] = row['value']
            end

            h.each do |id, value|
                pg_conn.exec("update counters set value = #{value.to_i + 1} where id = #{id}")
            end
            pg_conn.exec("commit")
        end
    end

    def validate
        res = pg_conn.exec("select id, value from counters;")
        wrong_counters = 0
        res.each do |row|
            wrong_counters += 1 if row['counter'] != WORKERS_NUM * NUMBER_OF_INCREMENTS
        end

        logger.info("Wrong counters: #{wrong_counters}")
    end

    private

end

Runner.new.run(test_case: AccountTranfersNonRepeatableRead.new, workers_num: WORKERS_NUM)