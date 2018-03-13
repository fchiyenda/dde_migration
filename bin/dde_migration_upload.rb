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
	if person['patient']['identifiers'].nil?
		formated_person['patient'] = {"identifiers" => []}
	else
  	formated_person['patient'] = person['patient']
  end
	formated_person['birthdate_estimated'] = person['dob_estimated'].to_i
	formated_person['addresses'] = {"home_district" => person['home_district'],
																	"home_ta" => person['home_ta'],
																  "home_village" => person['home_village'],
																  "current_district" => person['current_district'],
																  "current_ta" => person['current_ta'],
																  "current_village" => person['current_village'],
																  "current_residence" => person['current_residence']}
	formated_person['created_at'] = person['created_at']
	formated_person['updated_at'] = Time.now
	formated_person['type'] = "Person"
	formated_person['assigned_site'] = person['location_id']
	formated_person['attributes'] = {"occupation" => person['occupation'],
																	 "cell_phone_number" => person['cell_phone_number']}
	if person['primary_identifier'].blank?
		 #Do nothing
	elsif person['primary_identifier'].length == 6 && !person['primary_identifier'].match(/\A[a-zA-Z0-9]*\z/).nil?
		formated_person['_id'] = person['primary_identifier']
	end
	
  return formated_person
end

def upload_data_to_couch
	i = 0
	n = 0
	docs = []
	amount_of_docs_per_batch = 25_000
  a = count.first
  total_records = a['records'].to_i
	j = total_records / amount_of_docs_per_batch

	puts "Create view to query NPIDs"
    system("curl -X PUT http://fchiyenda:E21081988g@localhost:5984/dde_test/_design/npids --data-binary @npids.json")

  while i <= j do
	#Get person data in batches specified
			migration_patient_ids = []
			primary_npid_references = []
			inconflict_npid_references = []
			ids_for_query = []
			legacy_ids = {}


			puts "Getting person data from database please wait"
			person_data = get_person_data(n,amount_of_docs_per_batch)

		
			#Get all Legacy ids for the batch
			person_data.select{|x|migration_patient_ids << x['migration_patient_id']}
			legacy_id = get_legacy_data(migration_patient_ids.join(','))

			legacy_id.each{|legacy| legacy_ids[legacy['migration_patient_id']] = legacy['legacy_identifiers'].split(',').uniq!}

			
			#Get all related primary NPID data
			person_data.each do |x|
				next if x['primary_identifier'].blank?
				if x['primary_identifier'].length == 6 && !x['primary_identifier'].match(/\A[a-zA-Z0-9]*\z/).nil?
					primary_npid_references << x['primary_identifier']
				end
			end

			legacy_id.each do |x|
				next if x['identifier'].blank?
				if x['identifier'].length == 6 && !x['identifier'].match(/\A[a-zA-Z0-9]*\z/).nil?
					inconflict_npid_references << x['identifier']
				end
			end

			primary_npid_references.uniq!
			inconflict_npid_references.uniq!

			person_data.each do |person|
		  	puts "processing #{person['migration_patient_id']}"
		  	person['patient'] = {"identifiers" => legacy_ids[person['migration_patient_id']]}
		  	docs << convert_to_dde3_format(person)
		  end

				puts "Posting data to DDE3.0"
				n += person_data.count
				url = "localhost:5984/dde_person_test/_bulk_docs"
				person_json = {"docs" => docs}
			  person_json = person_json.to_json
				response = RestClient.post(url,"#{person_json}",content_type: :json)
			  puts "#{response.code} processing upto #{n} of #{total_records}"
=begin			  
			  #Update primary NPIDs
			  primary_npids = RestClient.get("http://localhost:5984/dde_test/_design/npids/_view/get_npids?keys=#{primary_npid_references}")
				primary_npids = JSON.parse(primary_npids)

				primary_npids.each{|k,v| puts v}

				raise ids_for_query.inspect
=end

			  docs.clear
			  person_json.clear
			  i += 1			   		
	end  
  puts "Created #{n} records"
end

upload_data_to_couch