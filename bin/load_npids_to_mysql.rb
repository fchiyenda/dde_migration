`rm #{Rails.root}/log/dde_load_npids_errors.log`
i = 1
data = ''
File.open('/home/fchiyenda/Documents/Boabab/DDE/dde_npid.sql', 'r').each_line do |line|
  puts "Processing record #{i}"
  data += "(#{line}),"
  if (i % 50_000).zero? || 72_601_742 == i
    data.chomp!(',')
    puts 'Loading data into MySQL'
    begin
      ActiveRecord::Base.connection.execute <<EOF
    	  INSERT INTO npids values #{data};
EOF
    rescue StandardError => e
      `echo "#{e}" >> #{Rails.root}/log/dde_load_npids_errors.log`
    end
    data.clear
  end
  i += 1
end

def create_table
  ActiveRecord::Base.connection.execute <<EOF
      CREATE TABLE npids (
      id serial PRIMARY KEY NOT NULL UNIQUE,
      couchdb_ref bigint NOT NULL,
      npid varchar(255) NOT NULL,
      version_number varchar(255) NOT NULL,
      assigned boolean NOT NULL DEFAULT 'f',
      created_at timestamp NOT NULL,
      updated_at timestamp NOT NULL);
EOF
end
