require 'json'

def get_ngoni_data
	person = ActiveRecord::Base.connection.select_all <<EOF
      SELECT n.value,p.data FROM EVR_ngoni.national_patient_identifiers n
      RIGHT JOIN EVR_ngoni.people p ON n.id = p.id WHERE n.value <> "";
EOF

person.each do |per|

	puts "processing #{per['value']}"

	person_data = JSON.parse(per['data'])

	if person_data['birthdate_estimated'] == true
  	dob_est = 1
  elsif person_data['birthdate_estimated'] == false
    dob_est = 0
  else
  	dob_est = person_data['birthdate_estimated']
  end

  test1 = "NULL," +
  				"\"#{per['value']}\"," +
  				"\"Ngoni\"," +
  				"\"#{person_data['names']['given_name'].to_s.gsub("'","\'\\\\\'\'")}\"," +
  				"\"#{person_data['names']['middle_name'].to_s.gsub("'","\'\\\\\'\'")}\"," +
  				"\"#{person_data['names']['family_name'].to_s.gsub("'","\'\\\\\'\'")}\"," +
  				"\"#{person_data['gender']}\"," +
				  "\"#{person_data['birthdate']}\"," +
				  "#{dob_est}," +
				  "\"#{person_data['addresses']['address1'].to_s.gsub("'","\'\\\\\'\'")}\"," + #closest landmark
				  "NULL," + #current_residence
				  "\"#{person_data['addresses']['city_village'].to_s.gsub("'","\'\\\\\'\'")}\"," + #current Village
				  "\"#{person_data['addresses']['current_ta'].to_s.gsub("'","\'\\\\\'\'")}\"," + #Not available in dde1
				  "\"#{person_data['addresses']['state_province'].to_s.gsub("'","\'\\\\\'\'")}\"," + #Current District
				  "\"#{person_data['addresses']['neighborhood_cell'].to_s.gsub("'","\'\\\\\'\'")}\"," + #Home Village
				  "\"#{person_data['addresses']['county_district'].to_s.gsub("'","\'\\\\\'\'")}\"," + # Home ta
				  "\"#{person_data['addresses']['address2'].to_s.gsub("'","\'\\\\\'\'")}\","  # Home District
		  	  if person_data.include?('attributes')
						test1 = test1 + "\"#{person_data['attributes']['country_of_residence'].to_s.gsub("'","\'\\\\\'\'")}\"," +
	         		"\"#{person_data['attributes']['citizenship'].to_s.gsub("'","\'\\\\\'\'")}\"," +
	         		"\"#{person_data['attributes']['occupation'].to_s.gsub("'","\'\\\\\'\'")}\"," +
	            "\"#{person_data['attributes']['home_phone_number']}\"," +
	            "\"#{person_data['attributes']['cell_phone_number']}\"," +
	            "\"#{person_data['attributes']['office_phone_number']}\","
					elsif person_data.include?('person_attributes')
					 	test1 = test1 + "\"#{person_data['person_attributes']['country_of_residence'].to_s.gsub("'","\'\\\\\'\'")}\"," +
				      "\"#{person_data['person_attributes']['citizenship'].to_s.gsub("'","\'\\\\\'\'")}\"," +
				      "\"#{person_data['person_attributes']['occupation'].to_s.gsub("'","\'\\\\\'\'")}\"," +
				      "\"#{person_data['person_attributes']['home_phone_number']}\"," +
				      "\"#{person_data['person_attributes']['cell_phone_number']}\"," +
				      "\"#{person_data['person_attributes']['office_phone_number']}\","
					end
		test1 = test1 + "\"#{person_data['created_at']}\"," +
				    "\"#{person_data['assigned_site']}\""
   

	 `echo '#{test1}' >> #{Rails.root}/log/evr_data.sql`
	 unless $? == 0
     exit
   end
end
end

get_ngoni_data