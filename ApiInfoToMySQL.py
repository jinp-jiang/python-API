#coding=utf-8
import json
import pymysql
import requests
from multiprocessing.dummy import Pool as ThreadPool

# 读取API-V1接口
def read_data(url):
	v1_headers = {
	"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
	"Accept-Encoding": "gzip, deflate, br",
	"Accept-Language": "zh-CN,zh;q=0.9",
	"Cache-Control": "max-age=0",
	"Connection": "keep-alive",
	"Cookie": "DMS-tags-session=FH9nnIgdumgx67rFChJIRp8f/VzvZz6XcG+tbd8hHUCCC3VTi+O0U/I1Ogv9M9cvi2Ile/S+6FS+bR3Ge4BF4aLfVGlz/meRDu9IjsfdPwjAIM4nTTWzL+mc11QqHaOQDZLVTkzQ8MlUMohCYo6l0R73MToBBwsg720+cLVDo86wh157m9DOtJ1CPAjNefWlGMDFzxmJB5zi8jSFgIyVWyx9yTEF2oxl",
	"Host": "dms-tags.jcdecaux.com",
	"Sec-Fetch-Dest": "document",
	"Sec-Fetch-Mode": "navigate",
	"Sec-Fetch-Site": "none",
	"Sec-Fetch-User": "?1",
	"Upgrade-Insecure-Requests": "1",
	"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36"
	}
	v1_ssion = requests.session()
	v1_data = v1_ssion.get(url,headers=v1_headers,verify=False).json()
	return v1_data

#检查键值是否为空，为空将键值置为None，否则返回键值
def check_key(k,key):
        if k in key:
		#print(key[k])
        	return key[k]        
        else:
        	result = None
        	return result

def extract_data(data): 
	players = len(data['results'])
	#print(players)
	player_list = []
	#根据josn文件分析出player数量
	for i in range(0,players): 
		player = {}
		mac = data['results'][i]['key']
		player['MAC'] = mac
		#print(player)
		lists = data['results'][i]['tags'] 
		tag_len = len(lists) 
		#print(tag_len)
		for tag in range(0,tag_len):
			if 'name' in data['results'][i]['tags'][tag]:
				key = data['results'][i]['tags'][tag]['name']
			else:
				key = None
				#print(key)
			if 'value' in data['results'][i]['tags'][tag]:
				value = data['results'][i]['tags'][tag]['value']
			else:
				value = None
				#print(value)
			player[key] = value
		#print(player)
		#筛选出包含cn-std-6072键值的机器
		if 'cn-std-6072' in player:
			player_list.append(player)
		else:
			print('not cn-std-6072')
	return player_list

#将API-V1的josn内容输出到MySQL
def v1_data_to_mysql():
	v1_url = "https://dms-tags.jcdecaux.com/api/tagging/v1/entities?columns=key-tags.name-tags.value-tags.entity_type-tags.organization&limit=1000&offset=0&order_by=!last_updated"
	v1_url_page2 = "https://dms-tags.jcdecaux.com/api/tagging/v1/entities?columns=key-tags.name-tags.value-tags.entity_type-tags.organization&limit=1000&offset=1000&order_by=!last_updated"
	data_page1 = read_data(v1_url)
	data_page2 = read_data(v1_url_page2)
	playerlist = extract_data(data_page1)
	playerlist_page2 = extract_data(data_page2)
	playerlist.extend(playerlist_page2)
	#print(playerlist)
	#print(len(playerlist))
	#连接数据库将列表内容存入MySQL
	conn = pymysql.connect(host='1',port=,user='',passwd='',db='')
	cur = conn.cursor()	
	cur.execute("""truncate table test""")
	conn.commit()
	cur.close()
	conn.close()
	for l in range(0,len(playerlist)):
		#print(playerlist[l]['computer_model'])
		conn = pymysql.connect(host='',port=,user='',passwd='',db='')
		cur = conn.cursor()
		query = "insert into test(MAC,platform_version,bs_player_version,computer_manufacturer,os_platform,computer_model,project_code_platform,config_display_manager,storage_type,config_daily_poweroff,graphic_adapter,config_smartsync_smartcontent,bios_version,bsplayer_infra) values ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')"
		MAC = check_key('MAC',playerlist[l])
		platform_version = check_key('platform_version',playerlist[l])
		bs_player_version = check_key('bs_player_version',playerlist[l])
		computer_manufacturer = check_key('computer_manufacturer',playerlist[l])
		os_platform = check_key('os_platform',playerlist[l])
		computer_model = check_key('computer_model',playerlist[l])
		project_code_platform = check_key('project_code_platform',playerlist[l])
		config_display_manager = check_key('config_display_manager',playerlist[l])		
		storage_type =  check_key('storage_type',playerlist[l])
		config_daily_poweroff = check_key('config_daily_poweroff',playerlist[l])
		graphic_adapter = check_key('graphic_adapter',playerlist[l])
		config_smartsync_smartcontent = check_key('config_smartsync_smartcontent',playerlist[l])
		bios_version = check_key('bios_version',playerlist[l])
		bsplayer_infra = check_key('bsplayer_infra',playerlist[l])
		#print(MAC)
		#print(platform_version)
		cur.execute(query.format(MAC,platform_version,bs_player_version,computer_manufacturer,os_platform,computer_model,project_code_platform,config_display_manager,storage_type,config_daily_poweroff,graphic_adapter,config_smartsync_smartcontent,bios_version,bsplayer_infra))
		conn.commit()
		cur.close()
		conn.close()
	return playerlist

def mac_list(list_players):
	players = len(list_players)
	print(players)
	mac = []
	for i in range(0,players):
		 mac.append(list_players[i]['MAC'])
	return mac

#将API-V2的josn内容根据API-V1的MAC输出到MySQL
def v2_data_to_mysql(list_players):
	v2_url = "https://push.monitoring-dms.jcdecaux.com/api/v3/provisioning/hosts/search?mac=%s"
	v2_headers = {
	"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
	"Accept-Encoding": "gzip, deflate, br",
	"Accept-Language": "zh-CN,zh;q=0.9",
	"Cache-Control": "max-age=0",
	"Connection": "keep-alive",
	"Cookie": "cookie信息",
	"Host": "push.monitoring-dms.jcdecaux.com",
	"Sec-Fetch-Dest": "document",
	"Sec-Fetch-Mode": "navigate",
	"Sec-Fetch-Site": "none",
	"Sec-Fetch-User": "?1",
	"Upgrade-Insecure-Requests": "1",
	"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36"
	}	
	v2_ssion = requests.session()
	v2_data = v2_ssion.get(v2_url % list_players,headers=v2_headers,verify=False).json()
	conn = pymysql.connect(host='',port=,user='',passwd='',db='')
	cur = conn.cursor()
	query = "update test set Hostname='{}',IP='{}',last_update='{}' where MAC='{}';"
	try:
		cur.execute(query.format(v2_data[0]['name'],v2_data[0]['ip'],v2_data[0]['update_ts'],v2_data[0]['mac']))
	except Exception as e:
		print(e)
	conn.commit()
	cur.close()
	conn.close()
	print(v2_data[0]['name'],v2_data[0]['update_ts'],v2_data[0]['ip'],v2_data[0]['mac'])
	v2_data = None

if __name__ == '__main__':
	list_players=v1_data_to_mysql()
	print(type(list_players))
	mac = mac_list(list_players)
	processPool = ThreadPool(processes=8)
	#try:
	processPool.map(v2_data_to_mysql,mac)
	#except Exception as e:
	#	print(e)
	processPool.close()
	processPool.join()
