# Libre-ary Roadmap

This is the roadmap for the libre-ary project. There are a number of fronts of development that need to be done. They are organized by category.

## Metadata Tracking

	- Store all metadata somewhere (local db)
	- options to backup metadata in libre-ary itself
	- how to do this nicely?

## Scheduling
	- support scheduling as many backups as needed (maybe one an hour max?)
	- Design lightweight agent which will run scheduling

## Backing-up Through Adapters
	- pretty simple -- design adapters. 
	- Desgin interface
	- implement interface for each adapter maybe in this order:
		- Local
		- SQL
		- AWS ones
		- Google Drive
		- Others

## Configuration Design and Management
	
	- Determine what information must be stored about each file
	- Design criticality levels (with open standard for custom levels)
	- Break into several config files for ease of use
		- User config (auth, personal info)
		- Object config (criticality levels, etc)
		- Metadata config