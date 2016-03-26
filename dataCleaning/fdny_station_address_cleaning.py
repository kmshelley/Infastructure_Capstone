#Clean FDNY station data
import csv

with open('E:/GoogleDrive/DataSciW210/Final/datasets/FDNY_Firehouse_Listing.csv','r') as fdny:
    reader = csv.DictReader(fdny)
    with open('E:/GoogleDrive/DataSciW210/Final/datasets/FDNY_Firehouse_Listing_cleaned.csv','w') as newfile:
        fieldnames=['FacilityName','FacilityAddress','Borough','geoAddress']
        writer = csv.DictWriter(newfile, fieldnames=fieldnames)
        writer.writeheader()
        #clean up the fire station data
        for row in reader:
            #strip whitespace from station name
            row['FacilityName'] = row['FacilityName'].strip()
            
            clean_addr = []
            for term in unicode(row['FacilityAddress'],errors='ignore').split():
                if term.find('-') > -1: term = term.split('-')[0].strip() #remove ranges of addresses
                clean_addr.append(term)
            
            row['geoAddress'] = (' ').join(clean_addr) + ', ' + row['Borough'] + ', New York'
            
            writer.writerow(row)
