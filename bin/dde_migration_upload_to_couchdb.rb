require "json"
require 'rest-client'

def count
	count = ActiveRecord::Base.connection.select_all <<EOF
		SELECT count(*) as records FROM dde_migration_master.dde_person;
EOF

return count
end

def get_person_data(offset,limit)
	person_data = ActiveRecord::Base.connection.select_all <<EOF
		SELECT * FROM dde_migration_master.dde_person limit #{offset},#{limit};
EOF

end


def get_legacy_data(migration_ids)
	duplicate_person_and_legacy_ids = ActiveRecord::Base.connection.select_all <<EOF
		SELECT migration_patient_id,group_concat(identifier) as legacy_identifiers FROM dde_migration_master.dde_person_identifiers WHERE migration_patient_id IN (#{migration_ids}) GROUP BY migration_patient_id;
EOF

end

def convert_to_dde3_format(person)
	formated_person = {}
	
	#Build json data:
	formated_person['names'] = {"family_name" => person['family_name'],"given_name" => person['given_name'],"middle_name" => person['middle_name']}
	formated_person['gender'] = person['gender']
	formated_person['birthdate'] = person['dob'].strftime("%Y-%m-%d") rescue nil
	formated_person['date_of_death'] = person['death_date'].strftime("%Y-%m-%d") rescue nil
  if person['legacy_ids'].blank?
		formated_person['patient'] = {"identifiers" => []}
	else
    formated_person['patient'] = { "identifiers" => (person['legacy_ids'].strip.split(',')).uniq }
  end
	formated_person['birthdate_estimated'] = person['dob_estimated'].to_i
	formated_person['addresses'] = {"home_district" => person['home_district'],
																	"home_ta" => person['home_ta'],
																  "home_village" => person['home_village'],
																  "current_district" => person['current_district'],
																  "current_ta" => person['current_ta'],
																  "current_village" => person['current_village'],
																  "current_residence" => person['current_residence']}
	formated_person['created_at'] = person['date_created']
	formated_person['updated_at'] = Time.now
	formated_person['type'] = "Person"
	formated_person['assigned_site'] = person['location_id']
	formated_person['attributes'] = {"occupation" => person['occupation'],
																	 "cell_phone_number" => person['cell_phone_number']}
	if person['primary_identifier'].blank?
		 #Do nothing
	elsif person['primary_identifier'].strip.length == 6 && !person['primary_identifier'].match(/\A[a-zA-Z0-9]*\z/).nil?
		formated_person['_id'] = person['primary_identifier'].strip
	end
  
  return formated_person
end

def upload_non_conflict_npids
	@i = 0
	docs = []
	primary_npids = []
  legacy_npids = []
	puts 'Processing Non Conflict NPIDs'
  person_data = ActiveRecord::Base.connection.select_all <<EOF
		SELECT * FROM dde_migration_master.dde_person
		GROUP BY primary_identifier
    HAVING count(*) = 1;
EOF
 total_num_of_records = person_data.count
 person_data.each do |person|
   docs << convert_to_dde3_format(person)
   primary_npids << person['primary_identifier']
   unless docs.last['patient']['identifiers'].blank?
	   docs.last['patient']['identifiers'].each do |legacy_id|
	     legacy_npids << legacy_id if legacy_id.length == 6 && !legacy_id.match(/\A[a-zA-Z0-9]*\z/).nil?
	   end
	 end
   @i += 1
   if docs.count == 10_000 || @i == total_num_of_records
     upload_data_to_couch(docs,primary_npids,legacy_npids)
     docs.clear
     primary_npids.clear
     legacy_npids.clear
   end
 end
end

def upload_data_to_couch(docs, primary_npids, legacy_npids)
  puts 'Posting data to DDE3.0'
  url = "#{@couchconfig['protocol']}://#{@couchconfig['host']}:"\
				"#{@couchconfig['port']}/"\
	      "#{@couchconfig['prefix']}_person_#{@couchconfig['suffix']}/_bulk_docs"
  person_json = { 'docs' => docs }
  person_json = person_json.to_json
  response = RestClient.post(url, person_json.to_s, content_type: :json)
  puts response.code.to_s

  # Update primary NPIDs
  primary_npids = `curl -H "Content-Type: application/json" -X POST "http://localhost:5984/dde_test/_design/npids/_view/get_npids" -d '{"keys":#{primary_npids}}'`
  primary_npids = JSON.parse(primary_npids)
  primary_npids = primary_npids['rows'].map { |key| key['id'] }
  assigned_npid_log = []
  assigned_npid_log << primary_npids
  # Get NPIDs to update
  puts 'Getting Primary NPIDs to update'
  primary_npids = `curl -H "Content-Type: application/json" -X POST "#{@couchconfig['protocol']}://#{@couchconfig['host']}:#{@couchconfig['port']}/#{@couchconfig['prefix']}_#{@couchconfig['suffix']}/_all_docs?include_docs=true" -d '{"keys":#{primary_npids}}'`
  primary_npids = JSON.parse(primary_npids)
  docs = []
  primary_npids['rows'].each do |npid|
  	npid['doc']['assigned'] = true
  	npid['doc']['updated_at'] = Time.now.to_s
    docs << npid['doc']
  end
  url = "#{@couchconfig['protocol']}://#{@couchconfig['host']}:"\
				"#{@couchconfig['port']}/"\
	      "#{@couchconfig['prefix']}_#{@couchconfig['suffix']}/_bulk_docs"
  npids_json = { 'docs' => docs }
  npids_json = npids_json.to_json
  response = RestClient.post(url, npids_json.to_s, content_type: :json)
  puts response.code.to_s
  `echo '#{assigned_npid_log.join(',')}' >> #{Rails.root}/log/npids_marked_assigned.log`
    
  unless legacy_npids.blank?
	   # Update conflict NPIDs
	  conflict_npids = `curl -H "Content-Type: application/json" -X POST "http://localhost:5984/dde_test/_design/npids/_view/get_npids" -d '{"keys":#{legacy_npids}}'`
	  conflict_npids = JSON.parse(conflict_npids)
	  conflict_npids = conflict_npids['rows'].map { |key| key['id'] }
	  conflict_npid_log = []
	  conflict_npid_log << conflict_npids
	  # Get NPIDs to update
	  puts 'Updating conflict NPIDs'
	  conflict_npids = `curl -H "Content-Type: application/json" -X POST "#{@couchconfig['protocol']}://#{@couchconfig['host']}:#{@couchconfig['port']}/#{@couchconfig['prefix']}_#{@couchconfig['suffix']}/_all_docs?include_docs=true" -d '{"keys":#{conflict_npids}}'`
	  conflict_npids = JSON.parse(conflict_npids)
	  docs = []
	  conflict_npids['rows'].each do |npid|
	  	npid['doc']['assigned'] = true
	  	npid['doc']['site_code'] = '000'
	  	npid['doc']['updated_at'] = Time.now.to_s
	    docs << npid['doc']
		end
	  url = "#{@couchconfig['protocol']}://#{@couchconfig['host']}:"\
					"#{@couchconfig['port']}/"\
		      "#{@couchconfig['prefix']}_#{@couchconfig['suffix']}/_bulk_docs"
	  conflict_npids_json = { 'docs' => docs }
	  conflict_npids_json = conflict_npids_json.to_json
	  response = RestClient.post(url, conflict_npids_json.to_s, content_type: :json)
	  puts response.code.to_s
	  `echo '#{conflict_npid_log.join(',')}' >> #{Rails.root}/log/npids_marked_inconflict.log`
	end
end

def start
  @couchconfig = YAML.load_file('config/couchdb.yml')
  @mysqlconfig = YAML.load_file('config/database.yml')
  `rm #{Rails.root}/log/npids_marked_assigned.log`
  `rm #{Rails.root}/log/npids_marked_inconflict.log`

  puts 'Create view to query NPIDs'

  system("curl -X PUT #{@couchconfig['protocol']}:"\
        "//#{@couchconfig['username']}"\
 	      ":#{@couchconfig['password']}@#{@couchconfig['host']}:"\
 	      "#{@couchconfig['port']}/#{@couchconfig['prefix']}"\
 	      "_#{@couchconfig['suffix']}/_design/npids --data-binary @npids.json")
  upload_non_conflict_npids  
end

start
