require 'logger'

class Runner
    def initialize
        @logger = Logger.new(STDOUT)
    end 

    def run(test_case:, workers_num:) 
        if ARGV.length == 0
            test_case.prepare
            run_workers(workers_num: workers_num)
            test_case.validate
            test_case.cleanup
        elsif ARGV.length == 1
            worker_num = ARGV[0].to_i
            wait_for_signal
            test_case.work(worker_num: worker_num)
        end
    end

    private 

    attr_reader :logger

    def wait_for_signal
        thr = Thread.new do
            this_thread = Thread.current
            Signal.trap('USR1') { this_thread.kill }
            sleep
        end
        thr.join
    end

    def run_workers(workers_num:)
        logger.debug('Spawning workers')

        processes = (0..workers_num).map do |worker_num|
            process_cmd = "ruby #{$0} #{worker_num}"
            logger.debug("Spawning #{process_cmd}")
            Process.spawn(process_cmd)
        end

        sleep 1
    
        logger.debug("Signaling workers")
        processes.each { |process| Process.kill('USR1', process) }

        logger.debug("Waiting for workers")
        processes.each { |process| Process.wait(process) }
    end

end
