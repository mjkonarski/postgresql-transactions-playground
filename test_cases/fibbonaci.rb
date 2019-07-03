require 'pg'
require 'ap'
require 'retryable'
load 'test_case.rb'
load 'runner.rb'

WORKERS_NUM = 2
PASSES = 20

#
# Fibbonaci 
#

class Fibbonaci < TestCase
    def prepare
        sql = <<SQL
        DROP TABLE IF EXISTS fibbo;
        create table fibbo  (
            id numeric,
            value bigint
        );
        insert into fibbo(id, value) values (1, 0);
        insert into fibbo(id, value) values (2, 1);
SQL
        pg_conn.exec(sql)
    end

    def work(worker_num:)
        PASSES.times do |i|
            exception_cb = proc do |exception|
                pg_conn.exec("rollback")
            end

            Retryable.retryable(tries: :infinite, exception_cb: exception_cb) do |retry_num|
                logger.debug("Worker #{worker_num}, pass: #{i}, retry_num: #{retry_num}")
                
                pg_conn.exec("begin transaction isolation level serializable")
            
                res = pg_conn.exec("select id, value from fibbo order by id desc limit 2")
                
                a_id = res.getvalue(1,0).to_i
                a = res.getvalue(1,1).to_i

                b_id = res.getvalue(0,0).to_i
                b = res.getvalue(0,1).to_i
                
                pg_conn.exec("insert into fibbo(id, value) values (#{b_id + 1}, #{a + b})")

                pg_conn.exec("commit")
             end
        end
    end

    def validate
        res = pg_conn.exec("select id, value from fibbo order by id")
       
        ids = []
        values = []
       
        res.each do |row|
            ids << row['id'].to_i
            values << row['value'].to_i
        end

        correct = true
        ids.each_with_index { |x, i| correct = false unless x == i + 1 }
        values[2..-1].each_with_index { |x, i| correct = false unless x == values[i] + values[i+1] }

        logger.info("Result is: #{correct}")
        
        ap ids
        ap values
    end

    private

end

Runner.new.run(test_case: Fibbonaci.new, workers_num: WORKERS_NUM)