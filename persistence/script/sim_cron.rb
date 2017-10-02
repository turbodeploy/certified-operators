require 'rubygems'
require 'date'

for d in 1..60 do
  for h in 1..24 do 

    print Time.now.to_s
    
    if Time.now.hour == 0 then
      puts " - running report generation"
      Thread.new { system "bash #{ENV['VMT_REPORTS_HOME']}/bin/run_all.sh" }
    else
      puts " - sleeping 1 hour"
    end

   STDOUT.flush()
   sleep 60*60
  end
end

