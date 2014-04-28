#!/usr/bin/ruby -w
require 'rubygems'
require 'optparse'
require 'date'
require 'mysql'

class HlrDb
  def initialize (dbhost, dbuser, dbpassword, dbname)
    @dbhost = dbhost
    @dbuser = dbuser
    @dbpassword = dbpassword
    @dbname = dbname
  end
end

class JTS < HlrDb
  def jts=(jts)
    @jts = jts
  end
  
  def dryrun=(dryrun)
    @dryrun= dryrun
  end
  
  def getSummaryTables
    begin
      con = Mysql.new @dbhost, @dbuser, @dbpassword, @dbname
      jts_tables = con.query('show tables like "summary_jts_%"')
      jts_tables
    rescue Mysql::Error => e
      printLog e.errno
      printLog e.error
    ensure
      con.close if con
    end
  end
  
  def createSummaryUnionView
    tables = self.getSummaryTables
    query = "CREATE OR REPLACE VIEW summary_jts AS "
    l = tables.num_rows
    i = 0
    tables.each do |t|
      i = i + 1
      query += "SELECT * from #{t}"
      if i < l 
        query += " UNION "
      end
    end
    con = Mysql.new @dbhost, @dbuser, @dbpassword, @dbname
    begin
      con.query(query)
    rescue Mysql::Error => e
      printLog e.errno
      printLog e.error
    ensure
      con.close if con
    end
  end
  
  def summarizePeriod(dateStart,dateEnd)
    begin
      con = Mysql.new @dbhost, @dbuser, @dbpassword, @dbname
      printLog con.get_server_info
      printLog "Will summarize records from:#{dateStart.to_s} -- #{dateEnd.to_s}, you've got 10 seconds to kill the command"
      sleep 10  
      s = DateTime.parse(dateStart)
      e = DateTime.parse(dateEnd)
      tableName = "summary_jts_#{s.strftime("%Y_%m_%d")}_#{e.strftime("%Y_%m_%d")}"
      lsString = s.strftime("%Y-%m-%d")
      leString = e.strftime("%Y-%m-%d")
      if (@dryrun)
        printLog "DRYRUN -- Aggregating: endDate >=#{lsString} and endDate<#{leString}"
      else
        queryStmt = con.prepare("create table #{tableName} AS SELECT siteName,
    date(endDate) AS record_date,
    userVo,substring_index(userFqan,';',1) as primFqan,
    gridUser,
    localUserId,
    voOrigin,
    count(*) as records,
    UNIX_TIMESTAMP(min(endDate)) as start_timestamp,
    UNIX_TIMESTAMP(max(endDate)) as end_timestamp,
    sum(wallTime)/3600 as wall_hrs,
    sum(cpuTime)/3600 as cpu_hrs,
    avg(iBench) as iBench    
    FROM hlr.jobTransSummary 
    WHERE endDate>= ? and endDate < ? 
    GROUP BY siteName,record_date,userVo,primFqan,gridUser,localUserId,voOrigin")
        queryStmt.execute(lsString,leString)
      end
      
    rescue Mysql::Error => e
      printLog e.errno
      printLog e.error
    ensure
      con.close if con
    end
  end
  
end


