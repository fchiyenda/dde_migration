require 'rest-client'
require 'json'
@i = 1
@data = ''

def get_evr_data(skip = 0)
  url =
    "http://#{@config['evr']['host']}:#{@config['evr']['couchdb_port']}" \
    "/#{@config['evr']['couchdb']}/_all_docs?include_docs=true&limit=50000" \
    "&skip=#{skip}"
  JSON.parse(RestClient.get(url, content_type: :json))['rows']
end

def convert_for_file(data,dob_est)
  data = 'DEFAULT,' \
         "\'#{data['_id']}\'," \
         "\'Mtema\'," \
         "\'#{data['doc']['names']['given_name'].to_s.gsub("'", "''")}\'," \
         "\'#{data['doc']['names']['middle_name'].to_s.gsub("'", "''")}\'," \
         "\'#{data['doc']['names']['family_name'].to_s.gsub("'", "''")}\'," \
         "\'#{data['doc']['gender']}\'," \
         "\'#{data['doc']['birthdate']}\'," \
         "#{data['doc']['birthdate_estimated']}," \
         "\'#{data['doc']['addresses']['closest_landmark'].to_s.gsub("'", "''")}\'," \
         "\'#{data['doc']['addresses']['current_residence'].to_s.gsub("'", "''")}\'," \
         "\'#{data['doc']['addresses']['current_village'].to_s.gsub("'", "''")}\'," \
         "\'#{data['doc']['addresses']['current_ta'].to_s.gsub("'", "''")}\'," \
         "\'#{data['doc']['addresses']['current_district'].to_s.gsub("'", "''")}\'," \
         "\'#{data['doc']['addresses']['home_village'].to_s.gsub("'", "''")}\'," \
         "\'#{data['doc']['addresses']['home_ta'].to_s.gsub("'", "''")}\'," \
         "\'#{data['doc']['addresses']['home_district'].to_s.gsub("'", "''")}\'," \
         "\'#{data['doc']['person_attributes']['country_of_residence'].to_s.gsub("'", "''")}\'," \
         "\'#{data['doc']['person_attributes']['citizenship'].to_s.gsub("'", "''")}\'," \
         "\'#{data['doc']['person_attributes']['occupation'].to_s.gsub("'", "''")}\'," \
         "\'#{data['doc']['person_attributes']['home_phone_number']}\'," \
         "\'#{data['doc']['person_attributes']['cell_phone_number']}\'," \
         "\'#{data['doc']['person_attributes']['office_phone_number']}\'," \
         "\'#{data['doc']['created_at']}\'," \
         "\'#{data['doc']['assigned_site']}\'"
end

def write_data_to_file(couch_data)
  couch_data.each do |person|
    puts "Processing ... #{person['doc']['_id']} .. record number #{@i}"
    if person['doc']['type'] == 'Person'
      if person['doc']['birthdate_estimated'] == 1
        dob_est = true
      elsif person['doc']['birthdate_estimated'] == 0
        dob_est = false
      end
      person_data = convert_for_file(person, dob_est)
      @data += "(#{person_data}),"
      if (@i % 300).zero? || @number_of_records.to_i == @i
        @data.chomp!(',')
        puts 'Loading data into PostgreSQL'
        begin
          ActiveRecord::Base.connection.execute <<EOF
            INSERT INTO evr_person values #{@data};
EOF
        rescue StandardError => e
          `echo "#{e}" >> #{Rails.root}/log/evr_load_into_flattable.log`
        end
          @data.clear
        end
    end
    @i += 1
  end
end

def create_table
  ActiveRecord::Base.connection.execute <<EOF
        DROP TABLE IF EXISTS evr_person;
EOF

    ActiveRecord::Base.connection.execute <<EOF
        DROP TABLE IF EXISTS evr_person_conflict;
EOF

  ActiveRecord::Base.connection.execute <<EOF
    CREATE TABLE evr_person(
     merge_id serial NOT NULL,
     identifier VARCHAR(255) NULL,
     source VARCHAR(255) NULL,
     given_name VARCHAR(255) NULL,
     middle_name VARCHAR(255) NULL,
    family_name VARCHAR(255) NULL,
    gender VARCHAR(255) NULL,
    dob DATE NULL,
    dob_estimated BOOLEAN NULL,
    closest_landmark VARCHAR(255) NULL,
    current_residence VARCHAR(255) NULL,
    current_village VARCHAR(255) NULL,
    current_ta VARCHAR(255) NULL,
    current_district VARCHAR(255) NULL,
    home_village VARCHAR(255) NULL,
    home_ta VARCHAR(255) NULL,
    home_district VARCHAR(255) NULL,
    country_of_residence VARCHAR(255) NULL,
    citizenship VARCHAR(255) NULL,
    occupation VARCHAR(255) NULL,
    home_phone_number VARCHAR(255) NULL,
    cell_phone_number VARCHAR(255) NULL,
    office_phone_number VARCHAR(255) NULL,
    created_at VARCHAR(255) NULL,
    assigned_site VARCHAR(255) NULL,  
    PRIMARY KEY (merge_id));
EOF

  ActiveRecord::Base.connection.execute <<EOF
    CREATE TABLE evr_person_conflict(
    merge_conflict_id serial NOT NULL,
    identifier VARCHAR(255) NULL,
    source VARCHAR(255) NULL,
    merge_id INT NOT NULL,
    conflict_reason INT NOT NULL,
    given_name VARCHAR(255) NULL,
    middle_name VARCHAR(255) NULL,
    family_name VARCHAR(255) NULL,
    gender VARCHAR(255) NULL,
    dob DATE NULL,
    dob_estimated BOOLEAN NULL,
    closest_landmark VARCHAR(255) NULL,
    current_residence VARCHAR(255) NULL,
    current_village VARCHAR(255) NULL,
    current_ta VARCHAR(255) NULL,
    current_district VARCHAR(255) NULL,
    home_village VARCHAR(255) NULL,
    home_ta VARCHAR(255) NULL,
    home_district VARCHAR(255) NULL,
    country_of_residence VARCHAR(255) NULL,
    citizenship VARCHAR(255) NULL,
    occupation VARCHAR(255) NULL,
    home_phone_number VARCHAR(255) NULL,
    cell_phone_number VARCHAR(255) NULL,
    office_phone_number VARCHAR(255) NULL,
    created_at VARCHAR(255) NULL,
    assigned_site VARCHAR(255) NULL,  
    PRIMARY KEY (merge_conflict_id));
EOF
end

def start
  puts 'Create tables in database'
  create_table
  @config = YAML.load_file('config/database.yml')
  url = "http://#{@config['evr']['host']}:#{@config['evr']['couchdb_port']}/" \
  "#{@config['evr']['couchdb']}/_all_docs?limit=1"
  @number_of_records =
    JSON.parse(RestClient.get(url, content_type: :json))['total_rows'].to_i
  counter = 0
  while counter <= @number_of_records
    puts "Getting data from couchdb from #{counter} to #{counter + 50_000}"
    write_data_to_file(get_evr_data(counter))
    counter += 50_000
  end
end
start
