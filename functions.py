from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt
import urllib3
from urllib3.util import Retry
import sys
import os
import time
from tqdm.notebook import tqdm, trange
import tqdm
import itertools
import threading

http = urllib3.PoolManager()

def productQuery(api,aoi,startDate,endDate):
    footprint = geojson_to_wkt(read_geojson(aoi))
    products = api.query(footprint,
                        date = (startDate,endDate),
                        platformname = 'Sentinel-2',
                        cloudcoverpercentage = (0, 10),
                        producttype='S2MSI2A')
    productIdList = []
    for product in products:
        productIdList.append(product)
    return productIdList

def isOnline(productId,headers):
    url = f"https://scihub.copernicus.eu/dhus/odata/v1/Products('{productId}')/Online/$value"
    resp = http.request('GET',url, headers=headers)
    if str(resp.data) =="b'true'":
        return True
    else:
        return False

def s2Download(api,productList):
    api.download_all(productList)

def requestAccess(productId,headers):
    url = f"https://scihub.copernicus.eu/dhus/odata/v1/Products('{productId}')/$value"
    resp = http.request('GET',url, headers=headers,retries=Retry(10))
    if int(resp.status) ==202:
        return True
    else:
        return False

def countdown(t):
    while t:
        mins, secs = divmod(t, 60)
        timer = '{:02d}:{:02d}'.format(mins, secs)
        print(f"Waiting for first product retrieval {timer}", end="\r")
        time.sleep(1)
        t -= 1

def s2AquireAll(parameters):
    done = False
    def animate():
        for c in itertools.cycle(['|', '/', '-', '\\']):
            if done:
                break
            sys.stdout.write('\rQuering from OpenAccess Hub  ' + c)
            sys.stdout.flush()
            time.sleep(0.1)
        sys.stdout.write('\rQuering Finished!             ')
    
    t = threading.Thread(target=animate)
    t.start()

    aoi = parameters['AOI']['geojson']
    startDate =  parameters['AOI']['startDate']
    endDate = parameters['AOI']['endDate']
    userName = parameters['parameters']['scihubUser']
    password = parameters['parameters']['scihubPassword']
    downloadDirectory = parameters['parameters']['dataPath']

    os.chdir(downloadDirectory)
    api = SentinelAPI(f"{userName}", f"{password}", 'https://apihub.copernicus.eu/apihub')
    headers = urllib3.make_headers(basic_auth=f"{userName}:{password}")

    productIds = productQuery(api,aoi,startDate,endDate)

    onlineProducts = []
    offlineProducts = []

    for productId in productIds:
        if isOnline(productId,headers):
            onlineProducts.append(productId)
        else:
            offlineProducts.append(productId)

    done = True

    print(f"\n{len(productIds)} Sentinel-2 Products are Queried and {len(onlineProducts)} are online while {len(offlineProducts)} are offline\n")

    if len(onlineProducts)==0:
        countdown(int(60*30))
    else:
        while len(offlineProducts)>0:
            print("Requesting offline products to reactivate ...\n")
            pbar = tqdm.tqdm(offlineProducts)
            online=0
            offline=0
            for product in pbar:
                for currentProduct in tqdm.tqdm([product],leave=False):
                    resp = requestAccess(currentProduct,headers) 
                pbar.set_description(f"Requesting {product} ")
                if resp :
                    online+=1
                else :
                    offline+=1
            pbar.set_description("Requesting Products Finished")
            print(f"{online} products accepted {offline} products Declined")

            print("Downloading online products ...\n")
            s2Download(api,onlineProducts)
            
            requestedProducts=offlineProducts

            onlineProducts = []
            offlineProducts = []

            for productId in requestedProducts:
                if isOnline(productId,headers):
                    onlineProducts.append(productId)
                else:
                    offlineProducts.append(productId)
    print("Downloading online products ...\n")
    s2Download(api,onlineProducts)


if __name__ == "__main__":
    paramFile = sys.argv[1]
    with open(paramFile,'r') as inputparams:
        params = eval(inputparams.read())
    s2AquireAll(params)
