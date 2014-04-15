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
  opt.banner = "Usage: record_post [OPTIONS] field=value ..."

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
  puts "#{options[:dateEnd].to_s} -- #{options[:dateStart].to_s}"
  s = DateTime.parse(options[:dateStart])
  e = DateTime.parse(options[:dateEnd])
  s.upto(e) do |day|
    lsString = day.strftime("%Y-%m-%d")
    leString = (day+1).strftime("%Y-%m-%d")
    printLog "SELECT uniqueChecksum,count(dgJobId) as count FROM jobTransSummary WHERE endDate >=? and endDate <? GROUP BY uniqueChecksum HAVING count(dgJobId) > 1, #{lsString},#{leString}"
    queryStmt = con.prepare("SELECT uniqueChecksum,count(dgJobId) as count FROM jobTransSummary WHERE endDate >=? and endDate <? GROUP BY uniqueChecksum HAVING count(dgJobId) > 1")
    queryStmt.execute(lsString,leString)
    printLog "Found #{queryStmt.num_rows} to DELETE in #{day} -> #{day+1}"  
    rows = []
    while row = queryStmt.fetch do
      if (options[:dryrun])
        printLog "DRYRUN -- #{row[0]}" if row[0]
      else
        if ( options[:verbose])
          printLog "DELETE FROM jobTransSummary WHERE uniqueChecksum=\"#{row[0]}\" AND accountingProcedure='outOfBand'" if row[0]
        end
        deleteStmt = con.prepare("DELETE FROM jobTransSummary WHERE uniqueChecksum=? AND accountingProcedure='outOfBand' AND endDate >=? AND endDate <?")
        deleteStmt.execute(row[0],lsString,leString) if row[0]
        deleteStmt2 = con.prepare("DELETE FROM jobTransSummary WHERE uniqueChecksum=? AND dgJobId LIKE '%/_' AND endDate >=? AND endDate <?")
        deleteStmt2.execute(row[0],lsString,leString) if row[0]
        deleteStmt3 = con.prepare("DELETE FROM jobTransSummary WHERE uniqueChecksum=? AND dgJobId LIKE '%/__' AND endDate >=? AND endDate <?")
        deleteStmt3.execute(row[0],lsString,leString) if row[0]
      end
    end
  end


rescue Mysql::Error => e
  printLog e.errno
  printLog e.error
ensure
con.close if con
end
puts

