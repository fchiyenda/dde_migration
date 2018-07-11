require 'csv'

def create_report
	villages = ActiveRecord::Base.connection.select_all <<EOF
	  select distinct current_village from evr_merge_person;
EOF

  evr_non_conflict = ActiveRecord::Base.connection.select_all <<EOF
	  select current_village,count(*) from evr_merge_person where source = 'evr'
	  group by current_village;
EOF

  evr_conflict = ActiveRecord::Base.connection.select_all <<EOF
	  select current_village,count(*) from evr_person_merge_conflict where source = 'evr'
	  group by current_village;
EOF

	ngoni_non_conflict = ActiveRecord::Base.connection.select_all <<EOF
	  select current_village,count(*) from evr_merge_person where source = 'ngoni'
	  group by current_village;
EOF

  ngoni_conflict = ActiveRecord::Base.connection.select_all <<EOF
	  select current_village,count(*) from evr_person_merge_conflict where source = 'ngoni' group by current_village;
EOF

evr_non_conflict_hash = {}
evr_conflict_hash = {}
ngoni_non_conflict_hash = {}
ngoni_conflict_hash = {}
for_table = {}
records = []

evr_non_conflict.each do |k|
	evr_non_conflict_hash[k['current_village']] = k['count']
end

evr_conflict.each do |k|
	evr_conflict_hash[k['current_village']] = k['count']
end

ngoni_non_conflict.each do |k|
	ngoni_non_conflict_hash[k['current_village']] = k['count']
end

ngoni_conflict.each do |k|
	ngoni_conflict_hash[k['current_village']] = k['count']
end

villages.each do |key|
	records <<  key['current_village']
end 

records.each do |village|
	for_table[village] = []
end

villages.each do |village|
			if evr_non_conflict_hash.include?("#{village['current_village']}")
		  for_table[village['current_village']] << evr_non_conflict_hash[village['current_village']]
		else
			for_table[village['current_village']] << 0
	  end
end

villages.each do |village|
			if evr_conflict_hash.include?("#{village['current_village']}")
		  for_table[village['current_village']][0] = for_table[village['current_village']][0] + evr_conflict_hash[village['current_village']]
		  end
end

villages.each do |village|
			if ngoni_non_conflict_hash.include?("#{village['current_village']}")
		  for_table[village['current_village']] << ngoni_non_conflict_hash[village['current_village']]
		else
			for_table[village['current_village']] << 0
	  end
end

villages.each do |village|
			if ngoni_conflict_hash.include?("#{village['current_village']}")
		  for_table[village['current_village']][1] = for_table[village['current_village']][1] + ngoni_conflict_hash[village['current_village']]
		  end
end

villages.each do |village|
			if evr_non_conflict_hash.include?("#{village['current_village']}")
		  for_table[village['current_village']] << evr_non_conflict_hash[village['current_village']]
		else
			for_table[village['current_village']] << 0
	  end
end

villages.each do |village|
			if ngoni_non_conflict_hash.include?("#{village['current_village']}")
		  for_table[village['current_village']] << ngoni_non_conflict_hash[village['current_village']]
		else
			for_table[village['current_village']] << 0
	  end
end

CSV.open("#{Rails.root}/log/evr_census_by_village.csv", "wb") {|csv| for_table.to_a.each {|elem| csv << elem} }
puts 'Done!'
end

create_report