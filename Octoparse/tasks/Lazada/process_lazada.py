#!/usr/bin/python3

import json
import csv
import time
import os
import sys
import glob
import getopt


def main(argv):
    folder = ''
    p_results_dir = ''
    platform = ''    
    try:
      opts, args = getopt.getopt(argv,"hp:f:u:",["platform","folder=","url="])
    except getopt.GetoptError:
        print('parse_lazada.py -f <folder location> -u <url (e.g https://www.lazada.co.id)>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('parse_lazada.py -p <platform (e.g LazadaID)> -f <folder location> -u <url (e.g https://www.lazada.co.id)>')
            sys.exit()
        elif opt in ("-p", "--platform"):
            platform = arg
        elif opt in ("-f", "--folder"):
            folder = arg
        elif opt in ("-u", "--url"):
            url = arg

    if folder.rfind("\/") != len(folder):
        folder = folder + "/"



    p_results_dir = folder + time.strftime('%m%d%Y') 


    if (folder == '' or p_results_dir == '' or platform == ''):
        sys.exit(2)

    if os.path.isdir(p_results_dir) == False:
    	os.makedirs(""+str(p_results_dir)+"")


    print('Output directory is ', p_results_dir)
    print('Platform is ', platform)

    extension = 'csv'
    os.chdir(folder)
    result = glob.glob('*.{}'.format(extension))

    print(result)
    csv.field_size_limit(100000000)

    data = []
    tempData = []
    i = 0
    for res in result:
        with open(folder + res, 'r', encoding="utf8") as file:

            reader = csv.reader(file)
            l = 1

            for row in reader:
                
                if(row[2] != 'searchkey' and row[0] != ''):
                   # try:
                    last_char = row[0][-1]
                    l = l + 1
                    

                    try:
                        raw = json.loads(row[0])
                    except Exception as e:
                        raw = []
                        print(str(l) + ' line error')

                     
                    for r in raw:
                        try:
                            tempData.append([r['name'], 'https:'+r['productUrl'], r['priceShow'], r['image'], r['sellerName'], url+'/shop/' + r['sellerName'].replace(" ", '-'), r['sku'], row[2]])
                            if len(tempData) >= 10000:
                                data.append(tempData)
                                tempData = []
                        except Exception as e:
                            print(str(l) + ' incomple data')



            if len(data) == 0 and len(tempData) > 0:
                data.append(tempData)

           
            while i < len(data):
                print(p_results_dir + '/' + platform + '('+str(i+1)+').csv')
                newData = data[i]
                with open(p_results_dir + '/' + platform + '('+str(i+1)+').csv', 'a', newline='', encoding="utf8", errors='ignore') as file:
                    writer = csv.writer(file)
                    writer.writerow(['results_name', 'results_url', 'results_price', 'results_image_url', 'results_seller', 'results_seller_url', 'results_itemnumber', 'searchkey'])
                    writer.writerows(newData)
                i = i + 1


        os.remove(folder + res)



      
if __name__ == "__main__":
   main(sys.argv[1:])


