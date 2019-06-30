require 'pg'
require 'ap'
load 'test_case.rb'
load 'runner.rb'

WORKERS_NUM = 2
NUMBER_OF_ACCOUNTS = 100
INITIAL_AMOUNT = 1000

# 
class AccountTranfersSeparateTransactions < TestCase
    def prepare
        sql = <<SQL
        DROP TABLE IF EXISTS accounts;
        create table accounts(
            id SERIAL,
            amount numeric
        );
        insert into accounts(amount) select #{INITIAL_AMOUNT} from generate_series(1, #{NUMBER_OF_ACCOUNTS});
SQL
        pg_conn.exec(sql)
    end

    def work(worker_num:)
        case worker_num
        when 1
            transfering_worker
        when 2
            validating_worker
        end
    end

    private

    def transfering_worker
        logger.info("TRANSFERING_WORKER starts")

        10000.times do 
            from_account = rand(1..NUMBER_OF_ACCOUNTS)
            to_account = rand(1..NUMBER_OF_ACCOUNTS)
            amount = rand(1..100)

            # two queries are not in the same transaction - that's wrong

            # solution:
            # pg_conn.exec('begin') 

            pg_conn.exec("update accounts set amount = amount - #{amount} where id = #{from_account}")
            pg_conn.exec("update accounts set amount = amount + #{amount} where id = #{to_account}")

            # pg_conn.exec('commit')             
        end

        logger.info("TRANSFERING_WORKER ends")
    end

    def validating_worker
        logger.info("VALIDATING_WORKER starts")
        expected_sum = INITIAL_AMOUNT * NUMBER_OF_ACCOUNTS
        logger.info("VALIDATING_WORKER: expected sum: #{expected_sum}")

        correct_sums = 0
        incorrect_sums = 0

        10000.times do 
            res = pg_conn.exec("select sum(amount) from accounts;")
            sum = res.getvalue(0, 0).to_i

            if sum == expected_sum
                correct_sums += 1
            else
                incorrect_sums += 1
            end
        end

        logger.info("Correct sums: #{correct_sums}")
        logger.info("Incrrect sums: #{incorrect_sums}")
    end

end

Runner.new.run(test_case: AccountTranfersSeparateTransactions.new, workers_num: WORKERS_NUM)