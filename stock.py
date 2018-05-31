import MySQLdb  as sql
import requests as req
import json as js
import sys as s
import time
import datetime as dt
import re
import html
import urllib.request
import os
import zipfile

def init():
	#Init
	load_db_data()
	#last_date = get_last_date()
	#last_date = "2018-05-23"
	#fetch_gainers("https://www.bseindia.com/markets/equity/EQReports/MktWatchR.aspx?filter=gainer*all$all$&Page=1")
	#process_records("G",last_date)
	#fetch_losers("https://www.bseindia.com/markets/equity/EQReports/MktWatchR.aspx?filter=loser*all$all$&Page=1")
	#process_records("L",last_date)
	#fetch_volume_data(last_date)
	#fetch_qty_traded(last_date)
	#fetch_bulk_deals(last_date)
	#bulk_scan()
	map_bulk_scan()
	return

def  map_bulk_scan():
	pr("I","Mapping Bulk Scan Data",1)
	#Fetch Map
	scrip_map = fetch_scrip_map()
	scrip_bnk = []
	db_obj    = sql_conn()
	cursor    = db_obj.cursor()
	qry       = "SELECT DISTINCT scrip_code FROM `bulk_deals` WHERE scrip_name =''"
	try:
		cursor.execute(qry)
		results = cursor.fetchall()
		for row in results:
			scrip_bnk.append(str(row[0]))
	except Exception as e:
		print("-E- "+str(e))
		print("-E- Error: unable to fecth data")
		s.exit()

	for scrip_code in scrip_bnk:
		if scrip_code in scrip_map:
			qry = "UPDATE bulk_deals set scrip_name='"+scrip_map[scrip_code]['N']+"' WHERE scrip_code='"+scrip_code+"'"
			execQuery(qry)
	return

def fetch_scrip_map():
	pr("I","Fetching Scrip Map",1)
	scrip_map = {}
	db_obj  = sql_conn()
	cursor  = db_obj.cursor()
	qry = "SELECT scrip_code,`group`,link FROM gainers"
	try:
		cursor.execute(qry)
		results = cursor.fetchall()
		for row in results:
			tmp = row[2].split("/")
			scrip_code  = tmp[6]
			scrip_name  = row[0]
			scrip_group = row[1]
			#print("Scrip Code -> "+scrip_code+" Scrip Name -> "+scrip_name+" Scrip Group -> "+scrip_group)
			scrip_map[str(scrip_code)] = {}
			scrip_map[str(scrip_code)]['N'] = scrip_name
			scrip_map[str(scrip_code)]['G'] = scrip_group
	except Exception as e:
		print("-E- "+str(e))
		print("-E- Error: unable to fecth data")
		s.exit()

	qry = "SELECT scrip_code,`group`,link FROM losers"
	try:
		cursor.execute(qry)
		results = cursor.fetchall()
		for row in results:
			tmp = row[2].split("/")
			scrip_code  = tmp[6]
			scrip_name  = row[0]
			scrip_group = row[1]
			#print("Scrip Code -> "+scrip_code+" Scrip Name -> "+scrip_name+" Scrip Group -> "+scrip_group)
			scrip_map[str(scrip_code)] = {}
			scrip_map[str(scrip_code)]['N'] = scrip_name
			scrip_map[str(scrip_code)]['G'] = scrip_group
	except Exception as e:
		print("-E- "+str(e))
		print("-E- Error: unable to fecth data")
		s.exit()
	
	db_obj.close()	
	return scrip_map

def bulk_scan():
	#Objective -> To scan any new scrip which does not exists on bulk_scan table
	db_bulk = fetch_bulk()
	db_all	= fetch_all_scrip()
	for scrip_code in db_all:
		if scrip_code not in db_bulk:
			pr("I","Scrip "+scrip_code+" Not In Bulk Table. Inserting....",1) 
			qry = "INSERT INTO bulk_scan VALUES ("+scrip_code+",'NO')"
			execQuery(qry)
	db_bulk = fetch_bulk()		
	for scrip_code in db_bulk:
		status = db_bulk[scrip_code]
		if status == "NO":
			pr("I","Data Does Not Exists For Scrip -> "+scrip_code+" Status -> "+status,1)
			fetch_hist_bulk_deals(scrip_code)
			#s.exit()		
	return

def fetch_hist_bulk_deals(scrip_code):
	pr("I","Fetching Data For  Scrip -> "+scrip_code,1)
	url     	= "https://www.bseindia.com/stock-share-price/stockreach_bulkblock.aspx?scripcode="+scrip_code+"&expandable=7"
	data_map 	= {}
	mon_map     = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06','Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}
	rawdata 	= fetch_url(url)
	for line in rawdata:
		if "TTRow" in line:
			tmp  = line.split(">")
			dat  = (tmp[1][:-4]).split(" ")
			clnt = tmp[3][:-4]
			clnt = re.sub(r"[^a-zA-Z0-9]+", ' ', clnt)
			typ  = tmp[5][:-4]
			qty  = (tmp[7][:-4]).replace(",","")
			prc  = (tmp[9][:-4]).replace(",","")
			date = dat[2]+"-"+mon_map[dat[1]]+"-"+dat[0]
			#print("date -> "+date+" client -> "+clnt+" type -> "+typ+" qty -> "+qty+" prc -> "+prc)
			if int(dat[2]) >= 2017:
				qry = "INSERT INTO bulk_deals VALUES('"+date+"',"+scrip_code+",'','"+clnt+"','"+typ+"',"+qty+","+prc+")"
				execQuery(qry)	
	qry = "UPDATE bulk_scan set status='YES' WHERE scrip_code = "+scrip_code
	execQuery(qry)		
	return

def fetch_all_scrip():
	pr("I","Loading DB Data For All Scrips",0)
	db_obj  = sql_conn()
	cursor  = db_obj.cursor()
	db_all  = {}
	qry     = "SELECT DISTINCT link,scrip_code FROM gainers";
	try:
		cursor.execute(qry)
		results = cursor.fetchall()
		for row in results:
			tmp = row[0].split("/")
			scrip_code = tmp[6]
			scrip_name = row[1]
			print("Scrip Code -> "+scrip_code+" Scrip Name -> "+scrip_name)
			db_all[str(scrip_code)] = scrip_name
	except Exception as e:
		print("-E- "+str(e))
		print("-E- Error: unable to fecth data")
		s.exit()
		qry     = "SELECT DISTINCT link,scrip_code FROM losers";
	try:
		cursor.execute(qry)
		results = cursor.fetchall()
		for row in results:
			tmp = row[0].split("/")
			scrip_code = tmp[6]
			scrip_name = row[1]
			#print("Scrip Code -> "+scrip_code+" Scrip Name -> "+scrip_name)
			db_all[str(scrip_code)] = scrip_name
	except Exception as e:
		print("-E- "+str(e))
		print("-E- Error: unable to fecth data")
		s.exit()
	db_obj.close()
	return db_all

def fetch_bulk():
	pr("I","Loading DB Data For Bulk Deals",0)
	db_obj  = sql_conn()
	cursor  = db_obj.cursor()
	db_bulk = {}
	qry    = "SELECT * FROM bulk_scan";
	try:
	    cursor.execute(qry)
	    results = cursor.fetchall()
	    for row in results:
	        
	        db_bulk[str(row[0])] = row[1]
	except:
	    print("-E- Error: unable to fecth data")
	    s.exit()
	db_obj.close()
	return db_bulk

def fetch_bulk_deals(last_date):
	db_bulk 	= load_bulk_data(last_date)
	pr("I","Fetching Bulk Deals Data",1)
	raw_data 	= fetch_url("https://www.bseindia.com/markets/equity/EQReports/bulk_deals.aspx")
	for line in raw_data:
		if "TTRow_right" in line:
			tmp   = line.split(">")
			scode = tmp[3][:-4]
			sname = tmp[5][:-4]
			clnt  = tmp[7][:-4]
			typ   = tmp[9][:-4]
			qty   = (tmp[11][:-4]).replace(",","")
			prc   = (tmp[13][:-4]).replace(",","")
			st    = last_date+"_"+scode+"_"+clnt
			if st not in db_bulk:
				qry = "INSERT INTO bulk_deals VALUES ('"+last_date+"',"+scode+",'"+sname+"','"+clnt+"','"+typ+"',"+qty+","+prc+")"
				execQuery(qry)
			#print("CODE -> "+scode +" Name -> "+sname+ " Client -> "+clnt+" Type -> "+typ+" Qty -> "+str(qty)+" Prc -> "+prc)
			#s.exit()
	return

def load_bulk_data(last_date):
	pr("I","Loading Bulk Deals Data For Date "+last_date,0)
	db_map  = {}
	db_obj  = sql_conn()
	cursor  = db_obj.cursor()
	qry     = "SELECT * FROM bulk_deals WHERE date='"+last_date+"'"
	try:
	    cursor.execute(qry)
	    results = cursor.fetchall()
	    for row in results:
	        st = str(row[0])+"_"+str(row[1])+"_"+str(row[3])
	        db_map[st] = 1
	except:
	    print("-E- Error: unable to fecth data")
	    s.exit()
	return db_map

def fetch_volume_data(last_date):
	pr("I","Fetching Volume Data For Date "+last_date,0)
	#https://www.bseindia.com/BSEDATA/gross/2018/SCBSEALL2305.zip
	date_arr = last_date.split("-")
	year 	 = date_arr[0]
	month    = date_arr[1]
	day      = date_arr[2]
	dir_path = os.path.dirname(os.path.abspath(__file__))
	file     = dir_path+"\BSE\Volume\\"+day+month+year+".zip"
	url = "https://www.bseindia.com/BSEDATA/gross/"+year+"/SCBSEALL"+day+month+".zip"
	urllib.request.urlretrieve (url, file)
	ctr = 1
	if os.path.exists(file):
		pr("I","File Successfully Downloaded",0)
		archive = zipfile.ZipFile(file, 'r')
		rawdata = str(archive.read("SCBSEALL"+day+month+".txt")).replace("\\r\\n",":")
		tmpdata = rawdata.split(":")
		tmpdata.pop(0)
		tmpdata.pop((len(tmpdata)-1))
		vol_map = {}
		for line in tmpdata:
			tmp  = line.split("|")
			code = tmp[1]
			qty  = int(tmp[2])
			val  = int(tmp[3])
			vol  = int(tmp[4])
			trn  = int(tmp[5])
			chg  = float(tmp[6])
			vol_map[code] 		 = {}
			vol_map[code]['QTY'] = str(qty)
			vol_map[code]['VAL'] = str(val)
			vol_map[code]['VOL'] = str(vol)
			vol_map[code]['TRN'] = str(trn)
			vol_map[code]['CHG'] = str(chg)
			ctr = ctr + 1
	else:	
		pr("E","File Downloading Failed",0)
		s.exit()

	pr("I","Parsed "+str(ctr)+" Records",0)
	db_map = load_vol_data(last_date)

	for scode in vol_map:
		st = last_date+"_"+scode
		if st not in db_map:
			pr("I",scode+" Data Not Available in DB For "+last_date,1)
			qry = "INSERT INTO volume VALUES ('"+last_date+"','"+scode+"',"+vol_map[scode]['QTY']+","+vol_map[scode]['VAL']+","+vol_map[scode]['VOL']+","+vol_map[scode]['TRN']+","+vol_map[scode]['CHG']+")"
			execQuery(qry)
	return

def load_vol_data(last_date):
	pr("I","Loading Volume Data For Date "+last_date,0)
	db_map = {}
	db_obj  = sql_conn()
	cursor  = db_obj.cursor()
	qry     = "SELECT * FROM volume WHERE date='"+last_date+"'"
	try:
	    cursor.execute(qry)
	    results = cursor.fetchall()
	    for row in results:
	        st = str(row[0])+"_"+str(row[1])
	        db_map[st] = row[2]
	except:
	    print("-E- Error: unable to fecth data")
	    s.exit()
	return db_map

def fetch_qty_traded(last_date):
	pr("I","Fetching Quantity Traded",0)
	scrip_map = {}
	#Load the data from DB here
	query   = "SELECT scrip_code,link FROM gainers WHERE date='"+last_date+"'"
	pr("Q","Query -> "+query,1)
	db_obj  = sql_conn()
	cursor  = db_obj.cursor()
	try:
	    cursor.execute(query)
	    results = cursor.fetchall()
	    for row in results:
	        scrip_map[row[0]] = row[1]
	except:
	    print("-E- Error: unable to fecth data")
	    s.exit()

	query = "SELECT scrip_code,link FROM losers WHERE date='"+last_date+"'"
	pr("Q","Query -> "+query,1)
	try:
	    cursor.execute(query)
	    results = cursor.fetchall()
	    for row in results:
	        scrip_map[row[0]] = row[1]
	except:
	    print("-E- Error: unable to fecth data")
	    s.exit()
	db_obj.close()
	vol_data = load_vol_data(last_date)
	for scrip,link in scrip_map.items():
		tmp    = link.split("/")
		scode  = tmp[6]
		st     = last_date+"_"+scode
		vol    = 0
		if st in vol_data:
			vol = vol_data[st]
		#print(scrip+" -> "+scode+" -> "+str(vol))
		if vol:
			qry = "UPDATE gainers set qty_traded="+str(vol)+" WHERE scrip_code ='"+scrip+"' AND date='"+last_date+"'"
			execQuery(qry)
			qry = "UPDATE losers set qty_traded="+str(vol)+" WHERE scrip_code ='"+scrip+"' AND date='"+last_date+"'"
			execQuery(qry)
	return

def get_last_date():
	pr("I","Getting Last Trading Date",0)
	raw_data 	= fetch_url("https://www.bseindia.com/BSEGraph/Graphs/GetSensexDatav1.aspx?index=Sensex")
	last_arr    = raw_data[0].split(",")
	last_date   = last_arr[0].replace("/","-")
	return last_date

def fetch_gainers(url):
	global prs_pg
	global gainers
	global prs_rc
	pr("I","Fetching Gainers Data For Page "+str(prs_pg),0);
	#pr("I","URL -> "+url,1);
	raw_data = fetch_url(url)
	#print(prs_rc)
	for line in raw_data:
		if "TTRow_right" in line:
			spl  	= line.split(" ")
			link 	= spl[8][6:-2]
			scode   = spl[9][22:-12]
			try:
				ltp     = spl[14][11:-8]
			except Exception as e:
				ltp  = 0
			grp     = spl[11][11:-8]
			chgp    = spl[17][11:-8]
			pchg    = spl[20][11:-6]
			if grp == "":
				try:
					grp   = spl[12][11:-8]
					ltp   = spl[16][11:-8]
					chgp  = spl[19][11:-8]
					pchg  = spl[23][11:-6]
				except Exception as e:
					scode   = spl[9][22:]+" "+spl[10][0:-12]
					ltp     = spl[15][11:-8]
					chgp    = spl[18][11:-8]
					pchg    = spl[21][11:-6]
			pstr    = "[Link -> "+link+"] [Code -> "+scode+"] [Group-> "+grp+"][LTP -> "+ltp+"] [Price Change -> "+chgp+"] [% Change -> "+pchg+"]"
			#pr("I",pstr,1)
			gainers[scode] = {};
			gainers[str(scode)]['LNK'] = link
			gainers[scode]['LTP'] = ltp
			gainers[scode]['CHP'] = chgp
			gainers[scode]['PCH'] = pchg
			gainers[scode]['GRP'] = grp
		if "MktWatchR.aspx" in line:
			if ">Next" in line:
				ln_num = line[134:-19]
				prs_pg = prs_pg+1
				if  ln_num in prs_rc:
					pr("I","Ignore Page "+ln_num+" As Already Parsed ",1);
					break
				else:
					prs_rc[ln_num] = 1
					fetch_gainers("https://www.bseindia.com/markets/equity/EQReports/MktWatchR.aspx?filter=gainer*all$all$&Page="+str(prs_pg))
	return

def fetch_losers(url):
	global prs_pg
	global losers
	global prs_rc
	pr("I","Fetching Losers Data For Page "+str(prs_pg),0);
	#pr("I","URL -> "+url,1);
	raw_data = fetch_url(url)
	for line in raw_data:
		if "TTRow_right" in line:
			spl  	= line.split(" ")
			link 	= spl[8][6:-2]
			scode   = spl[9][22:-12]
			try:
				ltp     = spl[14][11:-8]
			except Exception as e:
				print(spl)
				ltp  = 0
			grp     = spl[11][11:-8]
			chgp    = spl[17][11:-8]
			pchg    = spl[20][11:-6]
			if grp == "":
				try:
					grp   = spl[12][11:-8]
					ltp   = spl[16][11:-8]
					chgp  = spl[19][11:-8]
					pchg  = spl[23][11:-6]
				except Exception as e:
					scode   = spl[9][22:]+" "+spl[10][0:-12]
					ltp     = spl[15][11:-8]
					chgp    = spl[18][11:-8]
					pchg    = spl[21][11:-6]
					
			pstr    = "[Link -> "+link+"] [Code -> "+scode+"] [Group-> "+grp+"][LTP -> "+ltp+"] [Price Change -> "+chgp+"] [% Change -> "+pchg+"]"
			#pr("I",pstr,1)
			losers[scode] = {};
			losers[str(scode)]['LNK'] = link
			losers[scode]['LTP'] = ltp
			losers[scode]['CHP'] = chgp
			losers[scode]['PCH'] = pchg
			losers[scode]['GRP'] = grp
		if "MktWatchR.aspx" in line:
			if ">Next" in line:
				ln_num = line[133:-19]
				prs_pg = prs_pg+1
				if  ln_num in prs_rc:
					pr("I","Ignore Page "+ln_num+" As Already Parsed ",1);
					break
				else:
					prs_rc[ln_num] = 1
					fetch_losers("https://www.bseindia.com/markets/equity/EQReports/MktWatchR.aspx?filter=loser*all$all$&Page="+str(prs_pg))
	return

def fetch_url(url):
	ret = {}
	r = req.get(url)
	if(r.status_code == 200):
		pr("I","Fetching Successful",1);
		data = r.text
		ret  = data.split("\n")
	else:
		pr("I","Fetching Failed",1);
		exit()
	return ret

def pr(typ,msg,dbg):
	if dbg:
		if dbg_sw:
			print("-"+typ+"- "+msg)
	else:
			print("-"+typ+"- "+msg)
	return

def process_records(typ,date):
	pdic = {}
	cdif = {}
	tbl  = {}
	global prs_pg
	global prs_rc
	if typ == "G":
		pr("I","Processing Gainers",1)
		tbl  = "gainers"
		pdic = gainers
		cdic = db_gainers
	if typ == "L":
		pr("I","Processing Losers",1)
		tbl  = "losers"
		pdic = losers
		cdic = db_losers
	#Process the records here	
	for stk,val in pdic.items():
		#date = dt.datetime.today().strftime('%Y-%m-%d')
		ltp  = val['LTP']
		lnk  = val['LNK']
		chp  = val['CHP']
		pch  = val['PCH']
		grp  = val['GRP']
		if date+"_"+stk in cdic:
			pr("W","Data Already Exists For Scrip Code "+stk+" On "+date,0)
		else:
			pr("I","Inserting For Scrip Code "+stk+" On "+date,1)	
			qry = "INSERT INTO "+tbl+ " VALUES ('"+date+"','"+stk+"','"+grp+"',"+ltp+","+chp+","+pch+",'"+lnk+"',0)"
			execQuery(qry)
	prs_pg  = 1
	prs_rc  = {}
	return	

def load_db_data():
	pr("I","Loading DB Data For Gainers",0)
	db_obj  = sql_conn()
	cursor  = db_obj.cursor()
	qry    = "SELECT * FROM gainers";
	try:
	    cursor.execute(qry)
	    results = cursor.fetchall()
	    for row in results:
	        st = str(row[0])+"_"+str(row[1])
	        db_gainers[st] = 1
	except:
	    print("-E- Error: unable to fecth data")
	    s.exit()
	pr("I","Loading DB Data For Losers",0)   
	qry    = "SELECT * FROM losers";
	try:
	    cursor.execute(qry)
	    results = cursor.fetchall()
	    for row in results:
	        st = str(row[0])+"_"+str(row[1])
	        db_losers[st] = 1
	except:
	    print("-E- Error: unable to fecth data")
	    s.exit()
	db_obj.close()
	return

#DB Functions
def sql_conn():
	db = sql.connect("localhost","root","","stock")
	return db

def rcnt(qry):
    db_obj  = sql_conn()
    cursor  = db_obj.cursor()
    rows    = 0
    try:
        cursor.execute(qry)
        rows = cursor.rowcount
    except (sql.Error, sql.Warning) as e:
        print("-E- Query Failed")   
        print(e)
        db_obj.rollback()
    return rows

def execQuery(qry):
    pr("S","Executing Query "+qry,1)
    db_obj  = sql_conn()
    cursor  = db_obj.cursor()
    try:
        cursor.execute(qry)
        db_obj.commit()
    except (sql.Error, sql.Warning) as e:
        print("-E- Query Failed")   
        print(e)
        db_obj.rollback() 
    return
#Code Execution Starts Here
dbg_sw  	= 1
prs_pg  	= 1
prs_rc  	= {}
db_gainers 	= {}
db_losers  	= {}

gainers 	= {}
losers  	= {}
init()
#print(gainers)
#Code Execution Ends Here