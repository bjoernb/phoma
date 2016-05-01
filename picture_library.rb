# -- encoding: utf-8 --
require 'sqlite3'
require 'yaml'
require 'digest'
require 'logger'
require 'date'

class AppLogger
	def self.log
		if @logger.nil?	
			@logger = Logger.new "piclib.log"
			@logger.level = Logger::DEBUG
			@logger.progname = 'PicLib'
			@logger.datetime_format = '%Y-%m-%d %H:%M:%S '
		end
		@logger
	end
end
  
class PicLib
	
	VERSION = "1.0.0"

	# Database with picture library
	@@dbname = "piclib.db"
	
	# Picture data types to look for
	@@datatypes = "{jpeg,png,tif,bmp,jpg,jpeg}"

	@@opts = { :force_folder => false, :force_picture => false }
	
	#opts_accessor :force
	
	def initialize opts={}
		@opts = @@opts.merge opts
		AppLogger.log.warn "Opts are set to re-processes already processed directories (:force_folder => true)" if @opts[:force_folder]
		AppLogger.log.warn "Opts are set to re-analyse already analysed pictures (:force_picture => true)" if @opts[:force_picture]
		
		@db = SQLite3::Database.new @@dbname
		@db.execute "CREATE TABLE IF NOT EXISTS `Pictures` (
			`PictureKey`		TEXT NOT NULL UNIQUE,
			`PicturePath`		TEXT NOT NULL,
			`PictureName`		TEXT NOT NULL,
			`PictureDate`		INTEGER,
			`PictureRating`		INTEGER,
			`PictureExif`		TEXT,
			PRIMARY KEY(PictureKey)
			);"
		@db.execute "CREATE TABLE IF NOT EXISTS `Folders` (
			`FolderPath`		TEXT NOT NULL UNIQUE,
			PRIMARY KEY(FolderPath)
			);"
		@db.execute "CREATE TABLE IF NOT EXISTS `Duplicates` (
			`PictureKey`		TEXT NOT NULL,
			`PicturePath`		TEXT NOT NULL,
			`PictureName`		TEXT NOT NULL,
			`InsertDate`		TEXT NOT NULL,
			PRIMARY KEY(PicturePath)
			);"
	end
	
	def	process_pictures root_path
		AppLogger.log.info "----------------------------------------------------------------------------"
		AppLogger.log.info "Started processing root path #{root_path}"

		# Load already processed folders
		processed_folders = get_processed_folders
		
		# Iterate through each folder in the root path
		Dir.glob(File.join(root_path, '**/')).each do |directory|
		
			# Check if directory has already been processed
			if (!@opts[:force_folder] && processed_folders.include?(directory))
				AppLogger.log.warn "Directory has already been processed #{directory}"
				next
			end
		
			AppLogger.log.info "Started processing directory #{directory}"
			# Search all pictures in folder
			picture_paths = Dir.glob(File.join(directory, "*.#{@@datatypes}"))
			# Analyse all found pictures
			picture_paths.each do |picture_path|
				analyse_picture picture_path
			end
				
			# Set directory as 'processed'
			set_processed_folder directory
			AppLogger.log.info "Finished processing directory #{directory}"
		end
		AppLogger.log.info "Finished processing root path #{root_path}"
		AppLogger.log.info "----------------------------------------------------------------------------"
	end
	
	def analyse_picture picture_path
	
		AppLogger.log.debug "Analyse picture #{picture_path}"
		return if !File.file?(picture_path)
		begin
			# Get Picture attributes and calculate rating
			picture = MiniExiftool.new (picture_path)
			picture_rating = (picture.Rating == nil) ? 0 : Integer(picture.Rating)
			picture_key = Digest::SHA256.file(picture_path).hexdigest
			
			# If :force_picture is true, ensure that picture is deleted before
			@db.execute("DELETE FROM Pictures WHERE PicturePath = ?", picture_path) if @opts[:force_picture]
			
			# Insert picture with attributes into the db
			@db.execute("INSERT INTO Pictures (PictureKey, PicturePath, PictureName, PictureDate, PictureRating, PictureExif) VALUES(?, ?, ?, ?, ?, ?);",
				[picture_key, picture_path, File.basename(picture_path), picture.CreateDate.to_i, picture_rating, picture.to_yaml])
				
		rescue MiniExiftool::Error => e
			AppLogger.log.fatal e.message
		rescue SQLite3::ConstraintException => e
			# Duplicate alreade in database, look up if duplicate or identical
			picture_duplicate = @db.get_first_value("SELECT PicturePath FROM Pictures WHERE PictureKey = ?", picture_key)
			if picture_path == picture_duplicate
				AppLogger.log.warn "Picture is already stored in database #{picture_path}"
			else
				# Found a duplicate; write information to Duplicate table
				msg = "Found duplicate picture: #{picture_path}; is duplicate to: #{picture_duplicate}"
				puts msg
				AppLogger.log.error msg
				@db.execute("INSERT INTO Duplicates (PictureKey, PicturePath, PictureName, InsertDate) VALUE(?, ?, ?, ?);",
						[picture_key, picture_path, File.basename(picture_path), Date.today.to_s])
			end
		rescue SQLite3::Exception => e
			AppLogger.log.fatal e.message
		end
	end
	
	def get_processed_folders
		folders = Array.new
		begin
			@db.execute("SELECT FolderPath FROM Folders") do |row|
				folders << row[0]
			end
		rescue SQLite3::Exception => e
			AppLogger.log.fatal e.message
		end
		folders
	end
	
	def set_processed_folder folder_path
		begin
			@db.execute("INSERT INTO Folders (FolderPath) VALUES (?)", folder_path)
		rescue SQLite3::Exception => e
			return if e.message == "UNIQUE constraint failed: Folders.FolderPath"
			AppLogger.log.fatal e.message
		end
	end
	
	def find_pictures
		# Find star rated picture of the same week
		begin
			rs = @db.execute("SELECT PicturePath FROM Pictures WHERE strftime('%j', datetime(PictureDate, 'unixepoch')) = strftime('%j', julianday(?));", '2011-10-28')
			puts rs
		rescue SQLite3::Exception => e
			AppLogger.log.fatal e.message
		end
	end
end

#picture_root = "P:/bjoern/06 Familie"
#opts = { :force_folder => true }
#pl = PicLib.new opts
#pl.process_pictures picture_root

pl = PicLib.new
pl.find_pictures