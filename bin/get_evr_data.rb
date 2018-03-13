require 'rest-client'
require 'json'

def get_evr_data(skip = 0)
  url = "http://#{@config['evr']['host']}:#{@config['evr']['couchdb_port']}"\
  "/#{@config['evr']['couchdb']}/_all_docs?include_docs=true&skip="\
  "#{skip}&limit=100000"
  JSON.parse(RestClient.get(url, content_type: :json))['rows']
end

def convert_for_file(data,dob_est)
  data = "null," + 
         "\"#{data['doc']['_id']}\"," +
         "\"Mtema\"," +
         "\"#{data['doc']['names']['given_name'].to_s.gsub("'","\'\\\\\'\'")}\"," +
         "\"#{data['doc']['names']['middle_name'].to_s.gsub("'","\'\\\\\'\'")}\"," +
         "\"#{data['doc']['names']['family_name'].to_s.gsub("'","\'\\\\\'\'")}\"," +
         "\"#{data['doc']['gender']}\"," +
         "\"#{data['doc']['birthdate']}\"," +
         "#{dob_est}," +
         "\"#{data['doc']['addresses']['current_residence'].to_s.gsub("'","\'\\\\\'\'")}\"," +
         "\"#{data['doc']['addresses']['current_village'].to_s.gsub("'","\'\\\\\'\'")}\"," +
         "\"#{data['doc']['addresses']['current_ta'].to_s.gsub("'","\'\\\\\'\'")}\"," +
         "\"#{data['doc']['addresses']['current_district'].to_s.gsub("'","\'\\\\\'\'")}\"," +
         "\"#{data['doc']['addresses']['home_village'].to_s.gsub("'","\'\\\\\'\'")}\"," +
         "\"#{data['doc']['addresses']['home_ta'].to_s.gsub("'","\'\\\\\'\'")}\"," +
         "\"#{data['doc']['addresses']['home_district'].to_s.gsub("'","\'\\\\\'\'")}\"," +
         "\"#{data['doc']['person_attributes']['country_of_residence'].to_s.gsub("'","\'\\\\\'\'")}\"," +
         "\"#{data['doc']['person_attributes']['citizenship'].to_s.gsub("'","\'\\\\\'\'")}\"," +
         "\"#{data['doc']['person_attributes']['occupation'].to_s.gsub("'","\'\\\\\'\'")}\"," +
         "\"#{data['doc']['person_attributes']['home_phone_number']}\"," +
         "\"#{data['doc']['person_attributes']['cell_phone_number']}\"," +
         "\"#{data['doc']['person_attributes']['office_phone_number']}\"," +
         "\"#{data['doc']['created_at']}\"," +
         "\"#{data['doc']['assigned_site']}\""
  return data
end

def write_data_to_file(couch_data)
  couch_data.each do |person|
    puts "Processing ... #{person['doc']['_id']}"
    if person['doc']['type'] == 'Person' && person['doc']['addresses']['current_ta'] == 'Mtema'
      if person['doc']['birthdate_estimated'] == true
        dob_est = 1
      elsif person['doc']['birthdate_estimated'] == false
        dob_est = 0
      end
        person_data = convert_for_file(person,dob_est) 
        `echo '#{person_data}' >> #{Rails.root}/log/evr_data.sql`
        unless $? == 0
          exit
        end
    end
  end
end

def create_table
  ActiveRecord::Base.connection.execute <<EOF
      DROP DATABASE IF EXISTS #{@config['evr']['database']};
EOF

ActiveRecord::Base.connection.execute <<EOF
      CREATE DATABASE #{@config['evr']['database']};
EOF

ActiveRecord::Base.connection.execute <<EOF
  CREATE TABLE #{@config['evr']['database']}.evr_person(
  `ID`INT NOT NULL AUTO_INCREMENT,
  `identifier` VARCHAR(45) NULL,
  `source` VARCHAR(45) NULL,
  `given_name` VARCHAR(45) NULL,
  `middle_name` VARCHAR(45) NULL,
  `family_name` VARCHAR(45) NULL,
  `gender` VARCHAR(6) NULL,
  `dob` DATE NULL,
  `dob_estimated` BINARY NULL,
  `current_residence` VARCHAR(45) NULL,
  `current_village` VARCHAR(245) NULL,
  `current_ta` VARCHAR(245) NULL,
  `current_district` VARCHAR(45) NULL,
  `home_village` VARCHAR(245) NULL,
  `home_ta` VARCHAR(245) NULL,
  `home_district` VARCHAR(45) NULL,
  `country_of_residence` VARCHAR(45) NULL,
  `citizenship` VARCHAR(255) NULL,
  `occupation` VARCHAR(255) NULL,
  `home_phone_number` VARCHAR(255) NULL,
  `cell_phone_number` VARCHAR(255) NULL,
  `office_phone_number` VARCHAR(255) NULL,
  `created_at` VARCHAR(255) NULL,
  `assigned_site` VARCHAR(45) NULL,  
  PRIMARY KEY (`ID`));
EOF
end

def start
	`rm #{Rails.root}/log/evr_data.sql`
	@config = YAML.load_file('config/database.yml')
	url = "http://#{@config['evr']['host']}:#{@config['evr']['couchdb_port']}/#{@config['evr']['couchdb']}/_all_docs?limit=1"
	number_of_records = JSON.parse(RestClient.get(url,content_type: :json))['total_rows'].to_i	
	counter = 0
	while counter <= number_of_records
		puts "Getting data from couchdb from #{counter} to #{counter + 100_000}"
  	write_data_to_file(get_evr_data(counter))
  	counter +=100_000
  end
  puts "Create table in database"
  create_table
  puts "Loading data into table"
  `mysql -u#{@config['evr']['mysqlusername']} -p#{@config['evr']['mysqlpassword']} --local-infile -e \"LOAD DATA LOCAL INFILE '/var/www/dde_migration/log/evr_data.sql' INTO TABLE #{@config['evr']['database']}.evr_person FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n';\" ` 
end
start