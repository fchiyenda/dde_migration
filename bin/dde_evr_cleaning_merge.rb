def get_source_data
	source_data = ActiveRecord::base.connection.select_all <<EOF
		select * from evr_merge_person;
EOF

  source_data.each
  	scenario
end

def scenario1
end

def scenario2
end

def scenario3
end

def scenario4
end

def scenario5
end

def scenario6
end

def scenario7
end

def scenario8
end

def scenario9
end
