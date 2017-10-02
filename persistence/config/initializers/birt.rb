def birt4?()
  if(@birt4.nil?)
    begin
      birt = `rpm -qa birt-runtime*`
      ## e.g. "birt-runtime-4.2-3.i586\n", "birt-runtime-2.5-9.i586\n", etc.
      #p birt
      #birt_version = (/2\.5/ =~ birt) ? 'birt25' : 'birt42'
      #p birt_version
    rescue => e
      puts e
      birt = "2.5"
    end 
    @birt4 = (/2\.5/ =~ birt).nil?
  end
  return @birt4
end

IS_BIRT_4 = birt4?
#puts IS_BIRT_4.to_s


BIRT_XLS_EXT = IS_BIRT_4 ? 'xlsx' : 'xls'
#puts BIRT_XLS_EXT.to_s

#Available report extensions
REPORT_EXTS = %w(pdf xls xlsx)

#Compression used to store the reports
COMPRESSOR_EXT = 'gz'

#Available report extensions, compressed and uncompressed
REPORT_EXTS_W_CMPED = REPORT_EXTS.inject([]) do |res, ext| res<<ext; res<<"#{ext}.#{COMPRESSOR_EXT}"; end