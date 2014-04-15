#!/usr/bin/ruby -w
require 'rubygems'
require 'optparse'
require 'date'
require 'mysql'

options = {}
values = {}

def printLog (logS)
  currDate = DateTime.now
  puts "#{currDate.to_s} :  #{logS}"
end

opt_parser = OptionParser.new do |opt|
  opt.banner = "Usage: deleteRecordsByPeriod [OPTIONS]"

  options[:verbose] = false
  opt.on( '-v', '--verbose', 'Output more information') do
    options[:verbose] = true
  end

  options[:dryrun] = false
  opt.on( '-d', '--dryrun', 'Do not actually execute DELETEs') do
    options[:dryrun] = true
  end

  options[:dateStart] = nil
  opt.on( '-S', '--dateStart start', 'YYYY-MM-DD to start check duplicates') do |dateStart|
    options[:dateStart] = dateStart
  end

  options[:dateEnd] = nil
  opt.on( '-E', '--dateEnd end', 'YYYY-MM-DD to stop checking for duplicates') do |dateEnd|
    options[:dateEnd] = dateEnd
  end
  
  options[:sleepTime] = 30
  opt.on( '-s', '--sleep sleepTime', 'time to wait to allow other queries to be performed by DB') do |sleepTime|
    options[:sleepTime] = sleepTime
  end 

  opt.on( '-h', '--help', 'Print this screen') do
    puts opt
    exit
  end
end

opt_parser.parse!

ARGV.each do |f|
  f =~/(.*)=(.*)/
  data = Regexp.last_match
  values[data[1]] = data[2]
end

$stdout.sync = true

begin
  con = Mysql.new values['dbhost'], values['dbuser'], values['dbpasswd'], values['dbname']
  printLog con.get_server_info
  if (options[:dateEnd] == "now")
    options[:dateEnd] = DateTime.now.to_s
  end
  if (options[:dateStart] =~ /(.*)daysago/)
    options[:dateStart] = (DateTime.now-$1.to_i).to_s
  end
  puts "Will delete records from:#{options[:dateStart].to_s} -- #{options[:dateEnd].to_s}, you've got 10 seconds to kill the command"
  sleep 10
  s = DateTime.parse(options[:dateStart])
  e = DateTime.parse(options[:dateEnd])
  s.upto(e) do |day|
    lsString = day.strftime("%Y-%m-%d")
    leString = (day+1).strftime("%Y-%m-%d")
    printLog "Deleting records in date: #{lsString}"
    if (options[:dryrun])
        printLog "DRYRUN -- DELETE from jobTransSummary where endDate >=#{lsString} and endDate<#{leString}"
      else
        if ( options[:verbose])
          printLog "DRYRUN -- DELETE from jobTransSummary where endDate >=#{lsString} and endDate<#{leString}"
        end
        deleteStmt = con.prepare("DELETE FROM jobTransSummary WHERE endDate >=? AND endDate <?")
        deleteStmt.execute(lsString,leString)
      end
    sleep options[:sleepTime].to_i
  end


rescue Mysql::Error => e
  printLog e.errno
  printLog e.error
ensure
con.close if con
end
puts

