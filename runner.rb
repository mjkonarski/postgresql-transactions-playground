require 'logger'
require 'benchmark'

LOGLEVELS = %w[DEBUG INFO WARN ERROR FATAL UNKNOWN].freeze

class Runner
    def initialize
        loglevels = %w[DEBUG INFO WARN ERROR FATAL UNKNOWN].freeze
        @logger = Logger.new(STDOUT)
        @logger.level = loglevels.index(ENV.fetch("LOG_LEVEL","WARN")) || Logger::WARN

    end 

    def run(test_case:, workers_num:) 
        if ARGV.length == 0
            test_case.prepare
            time = Benchmark.measure { run_workers(workers_num: workers_num) }
            logger.info("Workers finished in #{time.real}")
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
        logger.debug("Spawning #{workers_num} workers")

        processes = (0...workers_num).map do |worker_num|
            process_cmd = "ruby #{$0} #{worker_num}"
            logger.info("Spawning #{process_cmd}")
            Process.spawn(process_cmd)
        end

        sleep 1
    
        logger.debug("Signaling workers")
        processes.each { |process| Process.kill('USR1', process) }

        logger.debug("Waiting for workers")
        processes.each { |process| Process.wait(process) }
        logger.info("Workers finished")

    end

end
