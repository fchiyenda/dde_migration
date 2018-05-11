require 'json'
require 'rest-client'

@start_at = Time.now()
@i = 0
@dump_filenum = 1

def get_npid_data(skip = 0)
  npids = JSON.parse(%x(curl "http://#{@config['dde_npids']['host']}:#{@config['dde_npids']['couchdb_port']}/#{@config['dde_npids']['couchdb']}/_all_docs?include_docs=true&skip=#{skip}&limit=200000"))['rows']
end

def get_npids_from_couchdb
  url = "http://#{@config['dde_npids']['host']}:#{@config['dde_npids']['couchdb_port']}/#{@config['dde_npids']['couchdb']}/_all_docs?limit=1"
  number_of_records = JSON.parse(RestClient.get(url,content_type: :json))['total_rows'].to_i  
  counter = 0
  while counter <= number_of_records
    puts "Getting data from couchdb from #{counter} to #{counter + 200_000}"
    npids = get_npid_data(counter)
    npids.each do |npid|
      create_migrate_to_dde_npids_to_mysql(npid['doc'])
    end
    counter +=200_000
  end
end

def create_migrate_to_dde_npids_to_mysql(dde_npid)
    @i += 1
    puts "processing.........#{@i}"
    if dde_npid.include?('site_code')
      `echo 'NULL,#{dde_npid['_id']},"#{dde_npid['national_id']}","V4",1,"#{Time.now().strftime('%Y-%m-%d %H:%M:%S')}","#{Time.now().strftime('%Y-%m-%d %H:%M:%S')}"' >> #{Rails.root}/log/dde_npid#{@dump_filenum}.sql`
      unless $? == 0
        `echo "#{dde_npid['_id']}" >> #{Rails.root}/log/dde_npid_migration_error.log`
      end
    else
      `echo 'NULL,#{dde_npid['_id']},"#{dde_npid['national_id']}","V4",0,"#{Time.now().strftime('%Y-%m-%d %H:%M:%S')}","#{Time.now().strftime('%Y-%m-%d %H:%M:%S')}"' >> #{Rails.root}/log/dde_npid#{@dump_filenum}.sql`
      unless $? == 0
        `echo "#{dde_npid['_id']}" >> #{Rails.root}/log/dde_npid_migration_error.log`
      end
    end
    @dump_filenum += 1 if (@i % 500_000.0).zero?
end

def start
  @config = YAML.load_file('config/database.yml')
  get_npids_from_couchdb
  puts "Merging dump files"
  (1..@dump_filenum).each do |i|
    `cat #{Rails.root}/log/dde_npid#{i}.sql >> #{Rails.root}/log/dde_npid.sql`
    `rm #{Rails.root}/log/dde_npid#{i}.sql`
  end
    
  puts "Script done: start at: #{@start_at.strftime('%d/%b/%Y %H:%M:%S')}, ended at: #{Time.now().strftime('%d/%b/%Y %H:%M:%S')}"
end
start
