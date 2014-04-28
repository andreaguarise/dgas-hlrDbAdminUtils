#!/usr/bin/ruby -w
require 'rubygems'
require 'optparse'
require 'date'
require 'mysql'
require 'hlrdb'

options = {}
values = {}

def printLog (logS)
  currDate = DateTime.now
  puts "#{currDate.to_s} :  #{logS}"
end

opt_parser = OptionParser.new do |opt|
  opt.banner = "Usage: produceJTSSummaries [OPTIONS]"

  options[:verbose] = false
  opt.on( '-v', '--verbose', 'Output more information') do
    options[:verbose] = true
  end

  options[:dryrun] = false
  opt.on( '-d', '--dryrun', 'Do not actually execute DELETEs') do
    options[:dryrun] = true
  end

  options[:months] = nil
  opt.on( '-M', '--Months months', 'Numbero of months to aggregate, one table per month will be produced') do |months|
    options[:months] = months
  end

  options[:dateEnd] = nil
  opt.on( '-E', '--dateEnd end', 'YYYY-MM-DD to stop producing summaries') do |dateEnd|
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
  if (options[:dateEnd] == "now")
    options[:dateEnd] = DateTime.now.to_s
  end
  if (options[:dateEnd] =~ /(.*)daysago/)
    options[:dateEnd] = (DateTime.now-$1.to_i).to_s
  end
  dateEnd = DateTime.new(DateTime.parse(options[:dateEnd]).year,DateTime.parse(options[:dateEnd]).month,1)
  #startDate = DateTime.parse(options[:dateEnd])
  jts = JTS.new values['dbhost'], values['dbuser'], values['dbpasswd'], values['dbname']
  jts.dryrun=options[:dryrun]
  i = 1
  while i < (options[:months].to_i + 1)
    dss = (dateEnd << i).strftime("%Y-%m-%d")
    des = (dateEnd << i -1).strftime("%Y-%m-%d")
    puts "From #{dss} to #{des}"
    jts.summarizePeriod(dss,des)
    i =  i + 1
  end
  

puts

