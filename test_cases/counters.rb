require 'pg'
require 'ap'
require 'retryable'
load 'test_case.rb'
load 'runner.rb'

WORKERS_NUM = 3
NUMBER_OF_COUNTERS = 100
NUMBER_OF_INCREMENTS = 1000

#
# Counters - workers are simultaneously trying to increment each of counters, by doing one select and many updates.
# The correct result is when all counters have value of WORKER_NUM * NUMBER_OF_INCREMENTS 
# 
# Solutions:
# 1. Optimistic locking. Run statements in REPEATABLE READ transaction. The transaction may fail with "could not serialize access due to concurrent update", 
# so we need to be able to retry it. No explit locking.
#
# 2. Pessimistic locking. Run statements in READ COMMITED transaction. Select rows with FOR UPDATE, but it's important to specify an order to prevent deadlocks.
# 
# 3. Do everything with one atomic statement
#
class Counters < TestCase
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
        NUMBER_OF_INCREMENTS.times do |i|
            exception_cb = proc do |exception|
                pg_conn.exec("rollback")
            end

            Retryable.retryable(tries: :infinite, exception_cb: exception_cb) do |retry_num|
                logger.debug("Worker #{worker_num}, iteration: #{i}, retry_num: #{retry_num}")
                
                # 1. 
                pg_conn.exec("begin isolation level repeatable read")
                # 2.                 
                # pg_conn.exec("begin")
                
                # 1. 
                res = pg_conn.exec("select id, value from counters;")
                # 2.
                # res = pg_conn.exec("select id, value from counters order by id for update;")
                
                h = {}
                res.each do |row|
                    h[row['id']] = row['value']
                end

                h.each do |id, value|
                    pg_conn.exec("update counters set value = #{value.to_i + 1} where id = #{id}")
                end

                pg_conn.exec("commit")

                # 3.
                # (1..NUMBER_OF_COUNTERS).each do |id|
                #     pg_conn.exec("update counters set value = value + 1 where id = #{id}")
                # end
            end
        end
    end

    def validate
        res = pg_conn.exec("select id, value from counters;")
        wrong_counters = 0
        res.each do |row|
            wrong_counters += 1 if row['value'].to_i != WORKERS_NUM * NUMBER_OF_INCREMENTS
        end

        logger.info("Correct counters: #{NUMBER_OF_COUNTERS - wrong_counters}")
        logger.info("Wrong counters: #{wrong_counters}")
    end

    private

end

Runner.new.run(test_case: Counters.new, workers_num: WORKERS_NUM)