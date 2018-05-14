require 'rest-client'
require 'json'
require 'mysql2'

def dbconnect(host,user,pwd)
   @cnn = Mysql2::Client.new(:host => "#{host}", :username => "#{user}",:password => "#{pwd}")
  end
  
def querydb(seqel)
   @rs = @cnn.query("#{seqel}")
end
  
def connect_to_mysqldb(h,u,p)
  dbconnect("#{h}","#{u}","#{p}")
end

def get_evr_data(skip = 0)
  url = "http://#{@config['evr']['host']}:#{@config['evr']['couchdb_port']}"\
  "/#{@config['evr']['couchdb']}/_all_docs?include_docs=true&skip="\
  "#{skip}&limit=100000"
  JSON.parse(RestClient.get(url, content_type: :json))['rows']
end

def insert_into_person_merge(person,source)
	ActiveRecord::Base.connection.execute <<EOF
      INSERT INTO evr_merge_person
      VALUES(DEFAULT,
      '#{person['_id']}',
      '#{source}',
      '#{person['names']['given_name']}',
      '#{person['names']['middle_name']}',
      '#{person['names']['family_name']}',
      '#{person['gender']}',
      '#{person['birthdate']}',
      '#{person['birthdate_estimated']}',
      '#{person['closest_landmark']}',
      '#{person['addresses']['current_residence']}',
      '#{person['addresses']['current_village']}',
      '#{person['addresses']['current_ta']}',
      '#{person['addresses']['current_district']}',
      '#{person['addresses']['home_village']}',
      '#{person['addresses']['home_ta']}',
      '#{person['addresses']['home_district']}',
      '#{person['addresses']['country_of_residence']}',
      '#{person['person_attributes']['citizenship']}',
      '#{person['person_attributes']['occupation']}',
      '#{person['person_attributes']['home_phone_number']}',
      '#{person['person_attributes']['cell_phone_number']}',
      '#{person['person_attributes']['office_phone_number']}',
      '#{person['created_at']}',
      '#{person['assigned_site']}');
EOF
end

def update_person_merge_conflict(person, merge_id, merge_reason,source)
	ActiveRecord::Base.connection.execute <<EOF
      INSERT INTO evr_person_merge_conflict
      VALUES(DEFAULT,
      	'#{person['_id']}',
      	'#{source}',
      	'#{merge_id.to_i}',
      	'#{merge_reason.to_i}',
      	'#{person['names']['given_name']}',
      	'#{person['names']['middle_name']}',
      	'#{person['names']['family_name']}',
      	'#{person['gender']}',
      	'#{person['birthdate']}',
      	'#{person['birthdate_estimated']}',
      	'#{person['closest_landmark']}',
      	'#{person['addresses']['current_residence']}',
      	'#{person['addresses']['current_village']}',
      	'#{person['addresses']['current_ta']}',
      	'#{person['addresses']['current_district']}',
      	'#{person['addresses']['home_village']}',
      	'#{person['addresses']['home_ta']}',
      	'#{person['addresses']['home_district']}',
      	'#{person['addresses']['country_of_residence']}',
      	'#{person['person_attributes']['citizenship']}',
      	'#{person['person_attributes']['occupation']}',
      	'#{person['person_attributes']['home_phone_number']}',
      	'#{person['person_attributes']['cell_phone_number']}',
      	'#{person['person_attributes']['office_phone_number']}',
      	'#{person['created_at']}',
      	'#{person['assigned_site']}');
EOF
end

def escape_apostrophes(person)
	person['doc']['names']['given_name'].to_s.gsub!("'","''")
	person['doc']['names']['middle_name'].to_s.gsub!("'","''")
	person['doc']['names']['family_name'].to_s.gsub!("'","''")
	person['doc']['addresses']['closest_landmark'].to_s.gsub!("'","''")
	person['doc']['addresses']['current_residence'].to_s.gsub!("'","''")
	person['doc']['addresses']['current_village'].to_s.gsub!("'","''")
	person['doc']['addresses']['current_district'].to_s.gsub!("'","''")
	person['doc']['addresses']['current_ta'].to_s.gsub!("'","''")
	person['doc']['addresses']['home_village'].to_s.gsub!("'","''")
	person['doc']['addresses']['home_district'].to_s.gsub!("'","''")
	person['doc']['addresses']['home_ta'].to_s.gsub!("'","''")

  return person
end

def scenario1(person)
ActiveRecord::Base.connection.select_all <<EOF
				SELECT * FROM evr_merge_person
				WHERE identifier = '#{person['_id']}'
				AND lower(given_name) = lower('#{person['names']['given_name']}')
				AND lower(middle_name) = lower('#{person['names']['middle_name']}')
				AND lower(family_name) = lower('#{person['names']['family_name']}')
				AND lower(gender) = lower('#{person['gender']}')
				AND dob = '#{person['birthdate']}'
				AND dob_estimated = '#{person['birthdate_estimated']}'
				AND lower(home_district) = lower('#{person['addresses']['home_district']}')
				AND lower(home_ta) = lower('#{person['addresses']['home_ta']}')
				AND lower(home_village) = lower('#{person['addresses']['home_village']}')
				AND lower(current_district) = lower('#{person['addresses']['current_district']}')
				AND lower(current_ta) = lower('#{person['addresses']['current_ta']}')
				AND lower(current_village) = lower('#{person['addresses']['current_village']}');
EOF
end

def scenario2(person)
	ActiveRecord::Base.connection.select_all <<EOF
				SELECT * FROM evr_merge_person
				WHERE lower(given_name) = lower('#{person['names']['given_name']}')
				AND lower(middle_name) = lower('#{person['names']['middle_name']}')
				AND lower(family_name) = lower('#{person['names']['family_name']}')
				AND lower(gender) = lower('#{person['gender']}')
				AND dob = '#{person['birthdate']}'
				AND dob_estimated = '#{person['birthdate_estimated']}'
				AND lower(home_district) = lower('#{person['addresses']['home_district']}')
				AND lower(home_ta) = lower('#{person['addresses']['home_ta']}')
				AND lower(home_village) = lower('#{person['addresses']['home_village']}')
				AND lower(current_district) = lower('#{person['addresses']['current_district']}')
				AND lower(current_ta) = lower('#{person['addresses']['current_ta']}')
				AND lower(current_village) = lower('#{person['addresses']['current_village']}');
EOF
end

def scenario3(person)
	ActiveRecord::Base.connection.select_all <<EOF
				SELECT * FROM evr_merge_person
				WHERE identifier = '#{person['_id']}'
				AND lower(given_name) = lower('#{person['names']['given_name']}')
				AND lower(middle_name) = lower('#{person['names']['middle_name']}')
				AND lower(family_name) = lower('#{person['names']['family_name']}')
				AND lower(gender) = lower('#{person['gender']}')
				AND dob = '#{person['birthdate']}'
				AND dob_estimated = '#{person['birthdate_estimated']}'
				AND lower(home_district) = lower('#{person['addresses']['home_district']}')
				AND lower(home_ta) = lower('#{person['addresses']['home_ta']}')
				AND lower(home_village) = lower('#{person['addresses']['home_village']}');
EOF
end

def scenario4(person)
  ActiveRecord::Base.connection.select_all <<EOF
				SELECT * FROM evr_merge_person
				WHERE lower(given_name) = lower('#{person['names']['given_name']}')
				AND lower(middle_name) = lower('#{person['names']['middle_name']}')
				AND lower(family_name) = lower('#{person['names']['family_name']}')
				AND lower(gender) = lower('#{person['gender']}')
				AND dob = '#{person['birthdate']}'
				AND dob_estimated = '#{person['birthdate_estimated']}'
				AND lower(home_district) = lower('#{person['addresses']['home_district']}')
				AND lower(home_ta) = lower('#{person['addresses']['home_ta']}')
				AND lower(home_village) = lower('#{person['addresses']['home_village']}');
EOF
end

def scenario5(person)
  ActiveRecord::Base.connection.select_all <<EOF
				SELECT * FROM evr_merge_person
				WHERE identifier = '#{person['_id']}'
				AND lower(given_name) = lower('#{person['names']['given_name']}')
				AND lower(middle_name) = lower('#{person['names']['middle_name']}')
				AND lower(family_name) = lower('#{person['names']['family_name']}')
				AND lower(gender) = lower('#{person['gender']}')
				AND dob = '#{person['birthdate']}'
				AND dob_estimated = '#{person['birthdate_estimated']}'
				AND lower(home_ta) = lower('#{person['addresses']['home_ta']}')
				AND lower(home_village) = lower('#{person['addresses']['home_village']}')
				AND lower(current_ta) = lower('#{person['addresses']['current_ta']}')
				AND lower(current_village) = lower('#{person['addresses']['current_village']}');
EOF
end

def scenario6(person)
	ActiveRecord::Base.connection.select_all <<EOF
				SELECT * FROM evr_merge_person
				WHERE lower(given_name) = lower('#{person['names']['given_name']}')
				AND lower(middle_name) = lower('#{person['names']['middle_name']}')
				AND lower(family_name) = lower('#{person['names']['family_name']}')
				AND lower(gender) = lower('#{person['gender']}')
				AND dob = '#{person['birthdate']}'
				AND dob_estimated = '#{person['birthdate_estimated']}'
				AND lower(home_ta) = lower('#{person['addresses']['home_ta']}')
				AND lower(home_village) = lower('#{person['addresses']['home_village']}')
				AND lower(current_ta) = lower('#{person['addresses']['current_ta']}')
				AND lower(current_village) = lower('#{person['addresses']['current_village']}');
EOF
end

def scenario7(person)
	ActiveRecord::Base.connection.select_all <<EOF
				SELECT * FROM evr_merge_person
				WHERE identifier = '#{person['_id']}'
				AND lower(given_name) = lower('#{person['names']['given_name']}')
				AND lower(middle_name) = lower('#{person['names']['middle_name']}')
				AND lower(family_name) = lower('#{person['names']['family_name']}')
				AND lower(gender) = lower('#{person['gender']}')
				AND dob = '#{person['birthdate']}'
				AND dob_estimated = '#{person['birthdate_estimated']}'
				AND lower(home_ta) = lower('#{person['addresses']['home_ta']}')
				AND lower(home_village) = lower('#{person['addresses']['home_village']}');
EOF
end

def scenario8(person)
	ActiveRecord::Base.connection.select_all <<EOF
				SELECT * FROM evr_merge_person
				WHERE lower(given_name) = lower('#{person['names']['given_name']}')
				AND lower(middle_name) = lower('#{person['names']['middle_name']}')
				AND lower(family_name) = lower('#{person['names']['family_name']}')
				AND lower(gender) = lower('#{person['gender']}')
				AND dob = '#{person['birthdate']}'
				AND dob_estimated = '#{person['birthdate_estimated']}'
				AND lower(home_ta) = lower('#{person['addresses']['home_ta']}')
				AND lower(home_village) = lower('#{person['addresses']['home_village']}');
EOF
end

def scenario9(person)
	ActiveRecord::Base.connection.select_all <<EOF
				SELECT * FROM evr_merge_person
				WHERE identifier = '#{person['_id']}'
				AND lower(given_name) = lower('#{person['names']['given_name']}')
				AND lower(middle_name) = lower('#{person['names']['middle_name']}')
				AND lower(family_name) = lower('#{person['names']['family_name']}')
				AND lower(gender) = lower('#{person['gender']}')
				AND dob = '#{person['birthdate']}'
				AND dob_estimated = '#{person['birthdate_estimated']}';
EOF
end

def scenario10(person)
ActiveRecord::Base.connection.select_all <<EOF
				SELECT * FROM evr_merge_person
				WHERE lower(given_name) = lower('#{person['names']['given_name']}')
				AND lower(middle_name) = lower('#{person['names']['middle_name']}')
				AND lower(family_name) = lower('#{person['names']['family_name']}')
				AND lower(gender) = lower('#{person['gender']}')
				AND dob = '#{person['birthdate']}'
				AND dob_estimated = '#{person['birthdate_estimated']}'
				AND lower(home_ta) = lower('#{person['addresses']['home_ta']}')
				AND lower(home_village) = lower('#{person['addresses']['home_village']}');
EOF
end

def scenario11(person)
ActiveRecord::Base.connection.select_all <<EOF
				SELECT * FROM evr_merge_person
				WHERE lower(given_name) = lower('#{person['names']['given_name']}')
				AND lower(middle_name) = lower('#{person['names']['middle_name']}')
				AND lower(family_name) = lower('#{person['names']['family_name']}')
				AND lower(gender) = lower('#{person['gender']}')
				AND dob = '#{person['birthdate']}'
				AND dob_estimated = '#{person['birthdate_estimated']}'
				AND lower(current_district) = lower('#{person['addresses']['current_district']}')
				AND lower(current_ta) = lower('#{person['addresses']['current_ta']}');
EOF
end

def check_against_merge_criteria(person,source)
	puts person.inspect
	person = update_person_dob(person)
	if !scenario1(person).blank?
    update_person_merge_conflict(person, scenario1(person).first['merge_id'], '1', source)
  elsif !scenario2(person).blank?
    update_person_merge_conflict(person, scenario2(person).first['merge_id'], '2', source)
  elsif !scenario3(person).blank?
    update_person_merge_conflict(person, scenario3(person).first['merge_id'], '3', source)
  elsif !scenario4(person).blank?
    update_person_merge_conflict(person, scenario4(person).first['merge_id'], '4', source)
  elsif !scenario5(person).blank?
    update_person_merge_conflict(person, scenario5(person).first['merge_id'], '5', source)
  elsif !scenario6(person).blank?
    update_person_merge_conflict(person, scenario6(person).first['merge_id'], '6', source)
  elsif !scenario7(person).blank?
    update_person_merge_conflict(person, scenario7(person).first['merge_id'], '7', source)
  elsif !scenario8(person).blank?
    update_person_merge_conflict(person, scenario8(person).first['merge_id'], '8', source)
  elsif !scenario9(person).blank?
    update_person_merge_conflict(person, scenario9(person).first['merge_id'], '9', source)
  elsif !scenario10(person).blank?
    update_person_merge_conflict(person, scenario10(person).first['merge_id'], '10', source)
  elsif !scenario11(person).blank?
    update_person_merge_conflict(person, scenario11(person).first['merge_id'], '11', source)
  else
    insert_into_person_merge(person, source)
  end
end

def update_person_dob(person)
#Check if day and month are both estimated
	if person['birthdate'].blank? || !person.key?('birthdate')
		check_date = ["1900","01","01"]
	else
		if person['birthdate'].include?('-') 
			check_date = person['birthdate'].split('-')
	  elsif person['birthdate'].include?('/')
			check_date = person['birthdate'].split('/')
		end 

		if check_date[0].include?('?') && check_date[1].include?('?')
			check_date[0] = '01'
			check_date[1] = '07'
		elsif check_date[0].include?('?')
			check_date[0] = '15'
	  elsif check_date[2].include?('?')
	  	check_date[0] = '01'
			check_date[1] = '01'
	  	check_date[2] = '1900'
	  else
	  	#Do nothing
	  end
	end
	check_date.each do |value|
		i = 0
		check_date[i] = '1900' if value == '0000'
	end
	dob = "#{check_date[2]}-#{check_date[1]}-#{check_date[0]}"

	person['birthdate'] = dob.to_date.strftime("%Y-%m-%d")
	return person
end

def convert_birthdate_estimated_to_boolean(person)
	
	if person['doc']['birthdate_estimated'].to_s == '1'
	  dob_est = true
	  person['doc']['birthdate_estimated'] = dob_est
	elsif person['doc']['birthdate_estimated'].to_s == '0'
	  dob_est = false
	  person['doc']['birthdate_estimated'] = dob_est
	end

	return person
end

def write_data_to_file(couch_data)
	couch_data.each do |person|
		puts "Processing ... #{person['doc']['_id']}"
    next unless person['doc']['type'] == 'Person' &&
                person['doc']['addresses']['current_ta'] == 'Mtema'
    person = convert_birthdate_estimated_to_boolean(person)
		person = escape_apostrophes(person)
		check_against_merge_criteria(person['doc'],"Mtema")
  end
end

def create_table
  ActiveRecord::Base.connection.execute <<EOF
      DROP TABLE IF EXISTS evr_merge_person;
EOF

  ActiveRecord::Base.connection.execute <<EOF
      DROP TABLE IF EXISTS evr_person_merge_conflict;
EOF

ActiveRecord::Base.connection.execute <<EOF
  CREATE TABLE evr_merge_person(
   merge_id SERIAL,
   identifier VARCHAR(45) NULL,
   source VARCHAR(45) NULL,
   given_name VARCHAR(45) NULL,
   middle_name VARCHAR(45) NULL,
  family_name VARCHAR(45) NULL,
  gender VARCHAR(20) NULL,
  dob DATE NULL,
  dob_estimated BOOLEAN NULL,
  closest_landmark VARCHAR(255) NULL,
  current_residence VARCHAR(45) NULL,
  current_village VARCHAR(245) NULL,
  current_ta VARCHAR(245) NULL,
  current_district VARCHAR(45) NULL,
  home_village VARCHAR(245) NULL,
  home_ta VARCHAR(245) NULL,
  home_district VARCHAR(45) NULL,
  country_of_residence VARCHAR(45) NULL,
  citizenship VARCHAR(255) NULL,
  occupation VARCHAR(255) NULL,
  home_phone_number VARCHAR(255) NULL,
  cell_phone_number VARCHAR(255) NULL,
  office_phone_number VARCHAR(255) NULL,
  created_at VARCHAR(255) NULL,
  assigned_site VARCHAR(45) NULL,  
  PRIMARY KEY (merge_id));
EOF

ActiveRecord::Base.connection.execute <<EOF
  CREATE TABLE evr_person_merge_conflict(
  merge_conflict_id SERIAL,
  identifier VARCHAR(45) NULL,
  source VARCHAR(45) NULL,
  merge_id INT NOT NULL,
  merge_reason INT NOT NULL,
  given_name VARCHAR(45) NULL,
  middle_name VARCHAR(45) NULL,
  family_name VARCHAR(45) NULL,
  gender VARCHAR(20) NULL,
  dob DATE NULL,
  dob_estimated BOOLEAN NULL,
  closest_landmark VARCHAR(255) NULL,
  current_residence VARCHAR(45) NULL,
  current_village VARCHAR(245) NULL,
  current_ta VARCHAR(245) NULL,
  current_district VARCHAR(45) NULL,
  home_village VARCHAR(245) NULL,
  home_ta VARCHAR(245) NULL,
  home_district VARCHAR(45) NULL,
  country_of_residence VARCHAR(45) NULL,
  citizenship VARCHAR(255) NULL,
  occupation VARCHAR(255) NULL,
  home_phone_number VARCHAR(255) NULL,
  cell_phone_number VARCHAR(255) NULL,
  office_phone_number VARCHAR(255) NULL,
  created_at VARCHAR(255) NULL,
  assigned_site VARCHAR(45) NULL,  
  PRIMARY KEY (merge_conflict_id));
EOF

end

def get_data_from_mysql
	 databases = ['openmrs_ngoni','openmrs_ukwe','openmrs_A25']
	 databases.each do |database|
	   connect_to_mysqldb('localhost','test','test')
	   location = querydb("SELECT property_value FROM #{database}.global_property WHERE property = 'current_health_center_id';")
	   location = location.first['property_value']
	   person_data = querydb("SELECT pi.patient_id,pi.identifier npid,identifier_type,
                      (SELECT group_concat(pi2.identifier) legacy
                      FROM #{database}.patient_identifier pi2
                      WHERE pi2.identifier_type = 2
                      AND pi.patient_id = pi2.patient_id
                      GROUP BY patient_id) legacy_ids,
                      pi.date_created,given_name,middle_name,family_name,gender,birthdate,birthdate_estimated,death_date,pa.*
                      FROM #{database}.patient_identifier pi
                      JOIN #{database}.person p
                      ON pi.patient_id = p.person_id
                      JOIN #{database}.person_name pn
                      ON pi.patient_id = pn.person_id
                      LEFT JOIN #{database}.person_address pa
                      ON pi.patient_id = pa.person_id
                      where length(pi.identifier) = 6 
                      AND pi.voided = 0 and identifier_type = 3 
                      group by pi.patient_id,pi.identifier;")

	   person_data.each do |p|
	     data = {"doc"=> {"_id" => p['npid'],
	     					 "assigned_site" => location,
	               "patient_assigned"=> true,
	               "person_attributes"=> {
	                     "country_of_residence"=>"",
								       "citizenship"=> (p['citizenship'] rescue nil),
								       "occupation"=> (p['occupation'] rescue nil),
								       "home_phone_number"=> (p['home_phone_number'] rescue nil),
								       "cell_phone_number"=>(p['cell_phone_number'] rescue nil),
								       "office_phone_number"=>(p['office_phone_number'] rescue nil) 
	  																	 },
	   										"gender"=>p['gender'],
											   "names"=>{
											       "given_name"=> p['given_name'],
											       "family_name"=> p['family_name'],
											       "middle_name"=> p['middle_name']
											       		   },
													   "patient"=> {
													       "identifiers"=> [
													       									(p['legacy_ids'] rescue nil)
													  										]
													   },
													   "birthdate"=> p['birthdate'].strftime('%Y-%m-%d'),
													   "birthdate_estimated"=>p['birthdate_estimated'],
													   "addresses"=>{
													       "current_residence"=> (p['address1'] rescue nil),
													       "current_village"=>(p['city_village'] rescue nil),
													       "current_ta"=> "",
													       "current_district"=>(p['state_province'] rescue nil),
													       "home_village"=>(p['neighborhood_cell'] rescue nil),
													       "home_ta"=> (p['county_district'] rescue nil),
													       "home_district"=> (p['address2'] rescue nil)
													   },
													   "updated_at"=>Time.now(),
													   "created_at"=> p['date_created'],
													   "type"=>"Person"
													}
												}
					person = convert_birthdate_estimated_to_boolean(data)
					person = escape_apostrophes(person)
					check_against_merge_criteria(update_person_dob(person['doc']),location)
	  end
	end
end

def start
	@config = YAML.load_file('config/database.yml')
	puts "Create table in database"
  create_table
	url = "http://#{@config['evr']['host']}:#{@config['evr']['couchdb_port']}/#{@config['evr']['couchdb']}/_all_docs?limit=1"
	number_of_records = JSON.parse(RestClient.get(url,content_type: :json))['total_rows'].to_i	
	counter = 0
	while counter <= number_of_records
		puts "Getting data from couchdb from #{counter} to #{counter + 100_000}"
  	write_data_to_file(get_evr_data(counter))
  	counter +=100_000
	end

#Getting data from MySQL Ngoni database
puts "Getting data from MySQL"
get_data_from_mysql
end

start