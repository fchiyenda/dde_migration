require 'json'
require 'rest-client'
require 'json'

@start_at = Time.now()
@i = 0
@dump_filenum = 1

def get_npid_data(skip = 0)
  npids = JSON.parse(%x(curl "http://#{@config['dde_npids']['host']}:#{@config['dde_npids']['couchdb_port']}/#{@config['dde_npids']['couchdb']}/_all_docs?include_docs=true&skip=#{skip}&limit=50000"))['rows']
end

def get_npids_from_couchdb
  url = "http://#{@config['dde_npids']['host']}:#{@config['dde_npids']['couchdb_port']}/#{@config['dde_npids']['couchdb']}/_all_docs?limit=1"
  number_of_records = JSON.parse(RestClient.get(url,content_type: :json))['total_rows'].to_i  
  counter = 0
  while counter <= number_of_records
    puts "Getting data from couchdb from #{counter} to #{counter + 50_000}"
    npids = get_npid_data(counter)
    data = ''
    npids.each do |npid|
      dde_npid = npid['doc']
      if dde_npid['national_id'].length == 6
        @i += 1
        puts "processing.........#{@i}"
        if dde_npid.include?('site_code')
          data +="(NULL,#{dde_npid['_id']}," \
                "'#{dde_npid['national_id']}','V4',1," \
                "'#{Time.now().strftime('%Y-%m-%d %H:%M:%S')}'" \
                ",'#{Time.now().strftime('%Y-%m-%d %H:%M:%S')}'),"
        else
          data +="(NULL,#{dde_npid['_id']}," \
                "'#{dde_npid['national_id']}','V4',0," \
                "'#{Time.now().strftime('%Y-%m-%d %H:%M:%S')}'" \
                ",'#{Time.now().strftime('%Y-%m-%d %H:%M:%S')}'),"
        end
        
        if (@i % 50_000).zero? || (72_601_742 - @i) < 50_000
          create_migrate_to_dde_npids_to_mysql(data.chomp(','))
          data.clear
        end  
      end
    end
    counter += 50_000
  end
end
                                                        
def create_migrate_to_dde_npids_to_mysql(dde_npids)
  puts "Insert to Mysql"
  ActiveRecord::Base.connection.execute <<EOF
     INSERT INTO npids VALUES #{dde_npids};
EOF

end

def start
  @config = YAML.load_file('config/database.yml')
  get_npids_from_couchdb
   
  puts "Script done: start at: #{@start_at.strftime('%d/%b/%Y %H:%M:%S')}, ended at: #{Time.now().strftime('%d/%b/%Y %H:%M:%S')}"
end
start
