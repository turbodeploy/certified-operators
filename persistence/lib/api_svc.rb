
require 'rexml/document'
require 'net/http'
require 'uri'
require 'cgi'
require 'rbconfig'

class ApiService

  ServiceLocation = "localhost"
  Prefix          = "/vmturbo/api?inv.c"
  Port            = Config::CONFIG['host_os'].to_s["darwin"] ? 8400 : 8080


  #
  # cmd = { :svc => "ServiceEntityService",
  #         :method => "getByExpr",
  #         :args => ["PhysicalMachine",".*","priceIndex|Produces|ProducesSize",".*utilization","#{plan_name}"]
  #       }
  #

  def xml_for(cmd)
    href = [Prefix, cmd[:svc], cmd[:method], cmd[:args]].flatten.join("&")

    resp = ""

    begin
      Net::HTTP.start(ServiceLocation, Port) {|http|
        req = Net::HTTP::Get.new(href)
        req.basic_auth 'guest', 'guest'
        response = http.request(req)
        resp = response.body
      }
    rescue => e
      puts "*** Error: " + e.message
      resp = "<exception><message>#{e.message}</message></exception>"
    end

    return resp
  end

end


# # #     main    # # #

if ARGV.include?("test1") then

  svc = ApiService.new

  cmd = { :svc => "ServiceEntityService", :method => "getByExpr",
          :args => ["PhysicalMachine",".*","priceIndex|Produces|ProducesSize",".*utilization"]
        }

  puts svc.xml_for(cmd)

end

