import os
import sys

import math
import time
import pickle

from urllib.request import urlretrieve
from multiprocessing import Pool
import flickrapi

api_key = 'a19d1be8aee5ffe772223a36825dceeb'
secret = 'f67c12be72003c30'


def download_flickr_img_url_by_tag(tags, pagenum = None, show_log = False, start_date = None, end_date = None):
    """
    Downloads information about images matching the tag(s) 
    
    Params:
        tags (str): Comma separated tag words to search with
        #tags_conjunct (Bool): Indicates if intended tag combination is 'AND' (True).
        #search_title (Bool): Indicates if should search within the image title (True).
        pagenum (int): Index (starting from 1) of the result page to fetch (None).                       
        show_log (Bool): Enables logging (False).
    
    Returns: 
        dictionary containing the query result
    """
    
    # search with 'AND'ed tags (conjunction of tags)
    tags_conjunct = True
    
    # do search within title
    search_title = True 
    
    
    # initialize flickr api
    flickr = flickrapi.FlickrAPI(api_key, secret,format='parsed-json',
                               store_token=False,
                               cache=True)
    
    # generate base configuration for the flickr query    
    config = get_flickr_search_base_config(pagenum)
    
    # add tag query specific params
    if search_title:
        config['text'] = " ".join(tags.split(","))
        
    config['tags'] = tags
    config['tag_mode'] = 'all' if tags_conjunct else 'any'

    if (not (start_date is None or end_date is None) ):
        config['min_upload_date'] = start_date
        config['max_upload_date'] = end_date
        
    # fetch image infos
    result = flickr.photos.search(**config)
    
    if(show_log):
        print(f"Downloading page: {pagenum}")

    return result


def download_flickr_img_by_group(group_name, pagenum = None, show_log = False):

    """
    Downloads information about images from a given Flickr group
    
    Params: 
        group_name (str): Name of the flickr group.
        pagenum (int): Index (starting from 1) of the result page to fetch (None).                       
        show_log (Bool): Enables logging (False).
    
    Returns: 
        dictionary containing the query result 
    """

    # initialize flickr api
    flickr = flickrapi.FlickrAPI(api_key, secret,format='parsed-json', store_token=False, cache=True)
    
    # look up the id of the group by provided group name
    groupinfo = flickr.urls.lookupGroup(url='https://www.flickr.com/groups/' + group_name + '/')
    
    # extract the group id from the result
    group_id = groupinfo.get('group').get('id')
        
    # generate base configuration for the flickr query    
    config = get_flickr_search_base_config(pagenum)

    # add the group id to the config
    config['group_id'] = group_id
    
    # retrieve information of the photos from the group
    result = flickr.groups.pools.getPhotos(**config)

    if(show_log):   
        print(f"Downloading page: {pagenum}")
    
    return result        
        

def get_flickr_search_base_config(pagenum):
    """
    Generate base param configuration for Flickr API query.
    
    Params:
        pagenum (int): Index (starting from 1) of the result page to fetch.  
    
    Returns: 
        A Python dictionary containing the configuration
    """
    
    config = dict(per_page=100,
                  content_type=1,
                  license='1,2,3,4,5,6,9,10',
                  extras='url_o,url_c,date_upload,date_taken',
                  sort = 'date-posted-asc',
                  #sort='relevance',
                 )

    # add pagenum to config if has valid value  
    if not pagenum is None:
        config['page'] = pagenum
        
    return config


def download_flickr_img(photos, download_dir='/disks/data/datasets/dloaded/', 
                        im_size = 'c', max_num = None, show_log = False, worker_id = None, max_retry_count = 3):
    """
    
    Downloads images from Flickr.
    
    Params: 
        photos (sequence): Sequence of 'photo' elements (python dictionary) from Flickr API image search result.
        download_dir (str): Location where the downloaded images will be stored. (current directory).
        im_size (str): One of 'm', 'c', 'o', 'b' etc. See https://www.flickr.com/services/api/misc.urls.html for details.
        max_num (int): Maximum number of images to download from the given sequence (None). 
        show_log (Bool): Enables logging (False).
        worker_id (int): For tracking the worker this invokation belongs to (None).
        
    Returns: Does not return any value    
    """
    
    if show_log and not worker_id is None:
        print(f"image downloader#{worker_id} received list of {len(photos)} images ")
        
    # create the download directory if does not exist
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
        
    # limit maximum number of images to be downloaded if was specified
    if max_num is None:
        pass
    else:
        photos = photos[:min(max_num, len(photos))]
    
    # build image URL from component information contained in the photo elements
    def build_im_url(photo, im_size):
        try:
            farm_id = photo['farm']
            server_id = photo['server']  
            im_id = photo['id'] 
            secret = photo['secret'] 
            im_sz = im_size
        except:
            return None
        
        # format: https://farm{farm-id}.staticflickr.com/{server-id}/{id}_{secret}_[mstzb].jpg
        return f'https://farm{farm_id}.staticflickr.com/{server_id}/{im_id}_{secret}_{im_sz}.jpg'  

    
    for photo in photos:
        #img_url = photo.get('url_c') or photo.get('url_o') or build_im_url(photo, im_size)
        img_url = build_im_url(photo, im_size)
    
        # move on if image is not available
        if img_url is None:
            continue
        
        # build image name from location directory and image id
        img_name = os.path.join(download_dir, img_url.split('/')[-1])
        
        retry_count = 0
        need_retry = True
        
        while retry_count < max_retry_count and need_retry:
            try:
                # download and save image
                urlretrieve(img_url, img_name)
                need_retry = False
            except Exception as e:
                # enforced delay not to overwhelm the Flickr API  
                retry_count += 1
                print('Retry attempt: ', retry_count)
                time.sleep(3)
                
                
        if show_log:
            print('Downloaded '+img_name)
        
        


def downloader_wrapper(arg):
    """
    Wrapper for the image downloader.
    
    Params:
        arg (tuple): arguments (iterable, kwargs) to be passed to the image downloader.
        
    Returns: forwards image downloader result.     
    """
    
    args, kwargs = arg
    
    return download_flickr_img(args, **kwargs)


def get_worker_args(num_processes, query_photos_dict, max_img_dload_cnt_per_worker, download_dir, im_size, max_num):
    """
        Creates a list of arguments for the image downloader processes
        
        Params:
            num_processes (int): The intended number of image downloader processes
            query_photos_dict (dictionary): Containing Flickr ids of the images as keys and 'photo' elements as values 
                               that contain information such as farm-id, secret, etc.
            max_img_dload_cnt_per_worker (int): The maximum number of images to be assigned to a worker process.
            download_dir (str): The base directory for saving the downloaded images. Each process has its own subdirectory
                          under this dir in which it saves the images.
            im_size (str): One of the available image sizes (e.g. 'c', 'm', 'b', 'o') in Flickr.
            
        Returns:
            A list, each element of which corresponds to the argument to be passed to an image downloader process. 
    """
    
    arg = []
    
    # Retrieve the photo elements as a list
    query_photos = list(query_photos_dict.values())
    
    for i in range(num_processes):
        # create the keyword arguments for the processes
        kwargs = {   
                     'download_dir' : os.path.join(download_dir,str(i+1)),
                     'im_size' : im_size,
                     'max_num' : max_num,
                     'worker_id': i+1,
                 }
        
        # compute the section end points' of the photo element list to be used for this worker process    
        photo_start_idx = max_img_dload_cnt_per_worker*i
        photo_end_idx = max_img_dload_cnt_per_worker*(i+1)
        
        # The regular argument and the keyword argument will be sent as argument to a process
        arg.append((query_photos[photo_start_idx:photo_end_idx], kwargs))        
        
    return arg