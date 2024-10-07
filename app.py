import os
import csv
import time

#Logger
from google.cloud import logging
logging_client = logging.Client()
log_name = "classifycnv-log"
logger = logging_client.logger(log_name)

from mongo import get_mongo_db, get_mongo_db2
client_db, db = get_mongo_db()
client_db2, db2 = get_mongo_db2()
logger.log_text("ClassifyCNV MongoDB connection opened")

#Save to Mongo, made
def save(title,row):
	filt = {"title": title}
	db["classifycnv"].replace_one(filt , row, upsert=True)
	db2["classifycnv"].replace_one(filt , row, upsert=True)
	logger.log_text(title + " ClassifyCNV value updated !")

#Convert Row to ACMG object
def row2acmg(title, row):

	#print(row)

	var_type = "loss"
	if row["Type"] == "DUP":
		var_type = "gain"
		
	score = float(row["Total score"])
	acmg_criteria = []
	if float(row["1A-B"]) != 0:
		acmg_criteria.append("1AB " + float(row["1A-B"]))
	if float(row["2A"]) != 0:
		acmg_criteria.append("2A " + float(row["2A"]))
	if float(row["2B"]) != 0:
		acmg_criteria.append("2B " + float(row["2B"]))
	if float(row["2C"]) != 0:
		acmg_criteria.append("2C " + float(row["2C"]))
	if float(row["2D"]) != 0:
		acmg_criteria.append("2D " + float(row["2D"]))
	if float(row["2E"]) != 0:
		acmg_criteria.append("2E " + float(row["2E"]))
	if float(row["2F"]) != 0:
		acmg_criteria.append("2F " + float(row["2F"]))
	if float(row["2G"]) != 0:
		acmg_criteria.append("2G " + float(row["2G"]))
	if float(row["2H"]) != 0:
		acmg_criteria.append("2H " + float(row["2H"]))
	if float(row["2I"]) != 0:
		acmg_criteria.append("2I " + float(row["2I"]))
	if float(row["2J"]) != 0:
		acmg_criteria.append("2J " + float(row["2J"]))
	if float(row["2K"]) != 0:
		acmg_criteria.append("2K " + float(row["2K"]))
	if float(row["2L"]) != 0:
		acmg_criteria.append("2L " + float(row["2L"]))
	if float(row["3"]) != 0:
		acmg_criteria.append("3 " + float(row["3"]))
	if float(row["4A"]) != 0:
		acmg_criteria.append("4A " + float(row["4A"]))
	if float(row["4B"]) != 0:
		acmg_criteria.append("42 " + float(row["4B"]))
	if float(row["4C"]) != 0:
		acmg_criteria.append("4C " + float(row["4C"]))
	if float(row["4D"]) != 0:
		acmg_criteria.append("4D " + float(row["4D"]))
	if float(row["4E"]) != 0:
		acmg_criteria.append("4E " + float(row["4E"]))
	if float(row["4F-H"]) != 0:
		acmg_criteria.append("4FH " + float(row["4F-H"]))
	if float(row["4I"]) != 0:
		acmg_criteria.append("4I " + float(row["4I"]))
	if float(row["4J"]) != 0:
		acmg_criteria.append("4J " + float(row["4J"]))
	if float(row["4K"]) != 0:
		acmg_criteria.append("4K " + float(row["4K"]))
	if float(row["4L"]) != 0:
		acmg_criteria.append("4L " + float(row["4L"]))
	if float(row["4M"]) != 0:
		acmg_criteria.append("4M " + float(row["4M"]))
	if float(row["4N"]) != 0:
		acmg_criteria.append("4N " + float(row["4N"]))
	if float(row["4O"]) != 0:
		acmg_criteria.append("4O " + float(row["4O"]))
	if float(row["5A"]) != 0:
		acmg_criteria.append("5A " + float(row["5A"]))
	if float(row["5B"]) != 0:
		acmg_criteria.append("5B " + float(row["5B"]))
	if float(row["5C"]) != 0:
		acmg_criteria.append("5C " + float(row["5C"]))
	if float(row["5D"]) != 0:
		acmg_criteria.append("5D " + float(row["5D"]))
	if float(row["5E"]) != 0:
		acmg_criteria.append("5E " + float(row["5E"]))
	if float(row["5F"]) != 0:
		acmg_criteria.append("5F " + float(row["5F"]))
	if float(row["5G"]) != 0:
		acmg_criteria.append("5G " + float(row["5G"]))
	if float(row["5H"]) != 0:
		acmg_criteria.append("5H " + float(row["5H"]))


	return {"title": title ,"chr":row["Chromosome"],"start":int(row["Start"]),"end":int(row["End"]),"var_type":var_type,
	"classification": row["Classification"], "score": score,
	"crt_1AB": float(row["1A-B"]),
	"crt_2A": float(row["2A"]),"crt_2B": float(row["2B"]),"crt_2C": float(row["2C"]),"crt_2D": float(row["2D"]),
	"crt_2E": float(row["2E"]),"crt_2F": float(row["2F"]),"crt_2G": float(row["2G"]),"crt_2H": float(row["2H"]),
	"crt_2I": float(row["2I"]),"crt_2J": float(row["2J"]),"crt_2K": float(row["2K"]),"crt_2L": float(row["2L"]),
	"crt_3": float(row["3"]),
	"crt_4A": float(row["4A"]),"crt_4B": float(row["4B"]),"crt_4C": float(row["4C"]),"crt_4D": float(row["4D"]),
	"crt_4E": float(row["4E"]),"crt_4FH": float(row["4F-H"]),"crt_4I": float(row["4I"]),"crt_4J": float(row["4J"]),
	"crt_4K": float(row["4K"]),"crt_4L": float(row["4L"]),"crt_4M": float(row["4M"]),"crt_4N": float(row["4N"]),
	"crt_4O": float(row["4O"]),
	"crt_5A": float(row["5A"]),"crt_5B": float(row["5B"]),"crt_5C": float(row["5C"]),"crt_5D": float(row["5D"]),
	"crt_5E": float(row["5E"]),"crt_5F": float(row["5F"]),"crt_5G": float(row["5G"]),"crt_5H": float(row["5H"]),
	"acmg_criteria" : acmg_criteria
	}

#Compute ACMG
def compute_acmg(batch_id, genomics_coordinates):

	#Write bed file
	f = open("/tmp/" + batch_id + ".bed",'w')
	for q in genomics_coordinates:
		dupdel = "DUP"
		if q["type"] == "loss":
			dupdel = "DEL"
		f.write(q["chr"] + "\t" + str(q["start"]) + "\t" + str(q["end"]) + "\t" + dupdel + "\n")
	f.close()
	
	ref = genomics_coordinates[0]["ref"]

	#Execute ClassifyCNV and convert result to regular dict
	os.system("python ClassifyCNV.py --infile /tmp/" + batch_id + ".bed --GenomeBuild " + ref + " --outdir /tmp/" + batch_id)
	if os.path.isfile("/tmp/" + batch_id + "/Scoresheet.txt"):
		with open("/tmp/" + batch_id + "/Scoresheet.txt") as csvfile:
			csvreader = csv.DictReader(csvfile, delimiter="\t")
			results = []
			for row in csvreader:
				var_type = "loss"
				if row["Type"] == "DUP":
					var_type = "gain"
				title = ref + "-" + row["Chromosome"] + "-" + str(row["Start"]) + "-" + str(row["End"]) + "-" + var_type
				res = row2acmg(title,row)
				save(title, res)
			return results
	else:
		return [] 

##############Entrypoint for GCP
from flask import Flask
from flask import request

app = Flask(__name__)

@app.route("/single", methods=["GET"])
def single():
	t = time.time()
	logger.log_text("ClassifyCNV Single")
	title = request.args.get("title")
	data = title.split("-")
	cnv = {"ref": data[0], "chr": data[1], "start": int(data[2]), "end": int(data[3]), "type": data[4]}
	compute_acmg(title, [cnv])
	logger.log_text(str(round(time.time() - t,2)) + " ClassifyCNV CNV-Hub finished !")
	return {"text":"ClassifyCNV Single OK !"}

@app.route("/batch", methods=["GET"])
def batch():
	t = time.time()
	logger.log_text("ClassifyCNV Batch")
	batch_id = request.args.get("batch-id")
	batch_data = db["cnvhub_batch"].find_one({'batchId':batch_id})["genomicCoordinates"]
	compute_acmg(batch_id, batch_data)
	logger.log_text(str(round(time.time() - t,2)) + " ClassifyCNV CNV-Hub finished !")
	return {"text":"ClassifyCNV Batch OK !"}

if __name__ == "__main__":
	app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))