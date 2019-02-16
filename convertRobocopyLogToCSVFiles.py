import re
import csv
from pprint import pprint
import datetime
import sys
import csv
import traceback

newFileRe = re.compile(r"\s+New File\s+([^\s][0-9\s\.mg]+)\s([^\n\r]+)")
#newFileRe = re.compile(r"\s+New File\s+([^\s].*)")
#errorRe = re.compile(r"([0-9/\s\:]\sERROR ([0-9]+)\s(.*))")
ignoreRe = re.compile(r"""(?:[\s]+$)|  #blank
                        (?:\-+[\r]$)| #hyphens
                        (?:\s+ROBOCOPY\s+\:\:.*$)| #ROBOCOPY header
                        (?:\s+Total\s+Copied\s+Skipped.*$)  #totals header
                        """,re.X)
blankRe = re.compile(r"[\s]*[\r]$")
errorRe = re.compile(r"(.*ERROR ([0-9]+)\s(.*))")
dividerRe = re.compile(r"\-+[\r]")
startedRe = re.compile(r".*Started \:.*?\,\s(.*)")
sourceRe = re.compile(r".*Source \:(.*)")
destRe = re.compile(r".*Dest \:(.*)")
filesRe = re.compile(r".*Files \:(.*)")
optionsRe = re.compile(r".*Options \:(.*)")
dirCountRe = re.compile(r"""\s+Dirs\s+\:\s*([0-9\.]+(?:\s[kmg])*)\s+
                                      ([0-9\.]+(?:\s[kmg])?)\s+
                                      ([0-9\.]+(?:\s[kmg])?)\s+
                                      ([0-9\.]+(?:\s[kmg])?)\s+
                                      ([0-9\.]+(?:\s[kmg])?)\s+
                                      ([0-9\.]+(?:\s[kmg])?)\s*""",re.X)
fileCountRe = re.compile(r"""\s+Files\s+\:\s*([0-9\.]+(?:\s[kmg])?)\s+
                                      ([0-9\.]+(?:\s[kmg])?)\s+
                                      ([0-9\.]+(?:\s[kmg])?)\s+
                                      ([0-9\.]+(?:\s[kmg])?)\s+
                                      ([0-9\.]+(?:\s[kmg])?)\s+
                                      ([0-9\.]+(?:\s[kmg])?\s*$)""",re.X)
byteCountRe = re.compile(r"""\s+Bytes\s+\:\s*([0-9\.]+(?:\s[kmg])?)\s+
                                      ([0-9\.]+(?:\s[kmg])?)\s+
                                      ([0-9\.]+(?:\s[kmg])?)\s+
                                      ([0-9\.]+(?:\s[kmg])?)\s+
                                      ([0-9\.]+(?:\s[kmg])?)\s+
                                      ([0-9\.]+(?:\s[kmg])?\s*$)""",re.X)
timesRe = re.compile(r"""\s+Times\s+\:\s*([0-9\:]+)\s+
                                      ([0-9\:]+)\s+
                                      ([0-9\:]+)\s+
                                      ([0-9\:]+)\s*\r$""",re.X)
speedBytesRe = re.compile(r"\s+Speed\s+\:\s+([0-9.]+)\sBytes.*$")
speedMegabytesRe = re.compile(r"\s+Speed\s+\:\s+([0-9.]+)\sMegaBytes.*$")
endedRe = re.compile(r"\s+Ended\s+\:\s+.*?\,\s+(.*(?:AM|PM)).*")

def convertRobocopyDateToBQFormat(datestring):
    datestring = datestring.strip()
    d = datetime.datetime.strptime(datestring,"%B %d, %Y %H:%M:%S %p")
    return d.strftime("%Y-%B-%d %H:%M:%S")

def getSizeFromRobosize(robosizeString):
    sizeStrings = robosizeString.strip().split(" ")
    if len(sizeStrings) == 1:
        return int(sizeStrings[0])
    if len(sizeStrings) == 2:
        if sizeStrings[1] == "g":
            return int(float(sizeStrings[0]) * 1000000000)
        if sizeStrings[1] == "m":
            return int(float(sizeStrings[0]) * 1000000)
        if sizeStrings[1] == "k":
            return int(float(sizeStrings[0]) * 1000)

def stripDictStrings(dict):
    for key,value in dict.items():
        if isinstance(value,str):
            dict[key] = value.strip()

class robocopyLogConverter(object):

    def __init__(self,filecsv,metacsv):
        self.backupMetadataDict = {}
        self.currentFileRecordDict = {}
        self.lineMatched = False
        self.fileCsvFields = ['Filename','Size','Type','Error','ErrorString',
                              'ErrorCode','BackupStartDate']
        self.metaCsvFields = ['Source','Dest','Started','Ended','Files','Options',
                              'bytescopied',
                              'bytesextras','bytesfailed','bytesmismatch','bytesskipped',
                              'bytestotal','dirscopied','dirsextras','dirsfailed',
                              'dirsmismatch','dirsskipped','dirstotal','filescopied',
                              'filesextras','filesfailed','filesmismatch','filesskipped',
                              'filestotal','speedBytes','speedMegaBytes','timescopied',
                              'timesextras','timesfailed','timestotal']
        





        self.fileCsvWriter = csv.DictWriter(filecsv,self.fileCsvFields,restval="")
        self.fileCsvWriter.writeheader()
        self.metaCsvWriter = csv.DictWriter(metacsv,self.metaCsvFields,restval="")
        self.metaCsvWriter.writeheader()
        
        
        self.robocopyRecordReAndHandlerList = [
            (newFileRe,self.newFileHandler),
            (errorRe,self.errorReHandler),
            (ignoreRe,self.ignoreReHandler),
            (dividerRe,self.dividerReHandler),
            (blankRe,self.blankReHandler),
            (startedRe,self.startedReHandler),
            (sourceRe,self.sourceReHandler),
            (destRe,self.destReHandler),
            (optionsRe,self.optionsReHandler),
            (dirCountRe,self.dirCountReHandler),
            (fileCountRe,self.fileCountReHandler),
            (byteCountRe,self.byteCountReHandler),
            (timesRe,self.timesReHandler),
            (speedBytesRe,self.speedBytesReHandler),
            (speedMegabytesRe,self.speedMegabytesReHandler),
            (endedRe,self.endedReHandler),  
            (filesRe,self.filesReHandler)  #filesRe needs to be after fileCountRe
        ]

        
    def blankReHandler(self,matchObject):
        print ("blank found")
        if self.currentFileRecordDict:
            self.writeFileRecord()

    def errorReHandler(self,matchObject):
        print("**********************errorhandler********************")
        self.currentFileRecordDict['ErrorCode'] = matchObject.group(1)
        self.currentFileRecordDict['Error'] = matchObject.group(2)
        return
    

    def ignoreReHandler(self,matchObject):
        print ("ignored line found")
        if self.currentFileRecordDict:
            self.writeFileRecord()

    def dividerReHandler(self,matchObject):
        print ("divider found")
        if self.currentFileRecordDict:
            self.writeFileRecord()

    def startedReHandler(self,matchObject):
        print ("started found")
        self.backupMetadataDict['Started'] = convertRobocopyDateToBQFormat(matchObject.group(1))
    def sourceReHandler(self,matchObject):
        print ("source found")
        self.backupMetadataDict['Source'] = matchObject.group(1).strip()

    def destReHandler(self,matchObject):
        print ("dest found")
        self.backupMetadataDict['Dest'] = matchObject.group(1).strip()

    def filesReHandler(self,matchObject):
        print ("files found")
        self.backupMetadataDict['Files'] = matchObject.group(1).strip()

    def optionsReHandler(self,matchObject):
        print ("options handler")
        self.backupMetadataDict['Options'] = matchObject.group(1).strip()

    def dirCountReHandler(self,matchObject):
        print("dircount handler")
        self.backupMetadataDict['dirstotal'] = getSizeFromRobosize(matchObject.group(1))
        self.backupMetadataDict['dirscopied'] = getSizeFromRobosize(matchObject.group(2))
        self.backupMetadataDict['dirsskipped'] = getSizeFromRobosize(matchObject.group(3))
        self.backupMetadataDict['dirsmismatch'] = getSizeFromRobosize(matchObject.group(4))
        self.backupMetadataDict['dirsfailed'] = getSizeFromRobosize(matchObject.group(5))
        self.backupMetadataDict['dirsextras'] = getSizeFromRobosize(matchObject.group(6))

    def fileCountReHandler(self,matchObject):
        print("filecount handler")
        self.backupMetadataDict['filestotal'] = getSizeFromRobosize(matchObject.group(1))
        self.backupMetadataDict['filescopied'] = getSizeFromRobosize(matchObject.group(2))
        self.backupMetadataDict['filesskipped'] = getSizeFromRobosize(matchObject.group(3))
        self.backupMetadataDict['filesmismatch'] = getSizeFromRobosize(matchObject.group(4))
        self.backupMetadataDict['filesfailed'] = getSizeFromRobosize(matchObject.group(5))
        self.backupMetadataDict['filesextras'] = getSizeFromRobosize(matchObject.group(6))

    def byteCountReHandler(self,matchObject):
        print("bytecount handler")
        self.backupMetadataDict['bytestotal'] = getSizeFromRobosize(matchObject.group(1))
        self.backupMetadataDict['bytescopied'] = getSizeFromRobosize(matchObject.group(2))
        self.backupMetadataDict['bytesskipped'] = getSizeFromRobosize(matchObject.group(3))
        self.backupMetadataDict['bytesmismatch'] = getSizeFromRobosize(matchObject.group(4))
        self.backupMetadataDict['bytesfailed'] = getSizeFromRobosize(matchObject.group(5))
        self.backupMetadataDict['bytesextras'] = getSizeFromRobosize(matchObject.group(6))

    def timesReHandler(self,matchObject):
        print ("times handler")
        self.backupMetadataDict['timestotal'] = matchObject.group(1).strip()
        self.backupMetadataDict['timescopied'] = matchObject.group(1).strip()
        self.backupMetadataDict['timesfailed'] = matchObject.group(1).strip()
        self.backupMetadataDict['timesextras'] = matchObject.group(1).strip()

    def speedBytesReHandler(self,matchObject):
        print("speed bytes handler")
        self.backupMetadataDict['speedBytes'] = matchObject.group(1).strip()

    def speedMegabytesReHandler(self,matchObject):
        print ("speed mega handler")
        self.backupMetadataDict['speedMegaBytes'] = matchObject.group(1).strip()

    def endedReHandler(self,matchObject):
        print ("ended handler")
        self.backupMetadataDict['Ended'] = convertRobocopyDateToBQFormat(matchObject.group(1))
        self.writeBackupMetadata()

    def newFileHandler(self,matchObject):
        if self.currentFileRecordDict:
            self.writeFileRecord()
        self.currentFileRecordDict['Type'] = "New"
        self.currentFileRecordDict['Filename'] = matchObject.group(2).strip()
        self.currentFileRecordDict['Size'] = getSizeFromRobosize(matchObject.group(1).strip())
        self.currentFileRecordDict['BackupStartDate'] = self.backupMetadataDict['Started']
        #print(self.currentFileRecordDict)
    

    def processLine(self,line):
        print("line: " + line)
        #print("---------------")
        for lineRe,reHandler in rlc.robocopyRecordReAndHandlerList:
            rlc.lineMatched = False
            matchObject = lineRe.match(line)
            if matchObject:
                rlc.lineMatched = True
                reHandler(matchObject)
                break
        print ("done with handlers")
        if not rlc.lineMatched:
            if rlc.currentFileRecordDict.get('ErrorCode'):
                #print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^setting ErrorString^^^^^^^^^^^^^^^^^^")
                if rlc.currentFileRecordDict.get('ErrorString'):
                    rlc.currentFileRecordDict['ErrorString'] = rlc.currentFileRecordDict['ErrorString'] + line
                else:
                    rlc.currentFileRecordDict['ErrorString'] = line
            else:
                print("No match:" + line)
        
    def writeFileRecord(self):
        stripDictStrings(self.currentFileRecordDict)
        self.currentFileRecordDict['BackupStartDate'] = self.backupMetadataDict['Started']
        #pprint(self.currentFileRecordDict)
        self.fileCsvWriter.writerow(self.currentFileRecordDict)
        self.currentFileRecordDict = {}

    def writeBackupMetadata(self):
        stripDictStrings(self.backupMetadataDict)
        self.metaCsvWriter.writerow(self.backupMetadataDict)
        


filename = sys.argv[1]
filecsvname = filename + "_files.csv"
metacsvname = filename + "_meta.csv"

try:
    with open(filename,'r',encoding='UTF-8',errors='ignore',newline='\n') as robolog, \
         open(filecsvname,'w') as filecsv, \
         open(metacsvname,'w') as metacsv:
        rlc = robocopyLogConverter(filecsv,metacsv)

        filerecord = ""
        for line in robolog:
            rlc.processLine(line)
except:
    print("exception")
    print(traceback.format_exc())



sys.exit(1)
                
whitespaceRE = re.compile("\s+")



datadict = {}
testcount = 100000000
i = 0
for line in file:
    try:
        if i > testcount:
            break
        i = i + 1
        if line[:24].find("New File") > 0:
            size,date,time,name = whitespaceRE.split(line[13:].strip(),3)
#            junk = line[:24]
#            size = line[13:25]
#            date = line[26:36]
#            time = line[37:45]
#            name = line[46:]
            (client,project) = getClientProjectPair(name)
            if not client:
                print ("client:", client, project)
                client = "None"
            if not project:
                print ("project:",client, project)
                project = "None"
            moddate = datetime.datetime.strptime(date,'%Y/%m/%d')
            numericsize = getSizeFromRobosize(size)
            data = datadict.get((client,project),None)
            if not data:
                datadict[(client,project)] = [1,numericsize,moddate]
            else:
                data[0] = data[0] + 1
                data[1] = data[1] + numericsize
                if data[2] < moddate:
                    data[2] = moddate
    except Exception as e:
        print ('XXXXXXX',size,date,time)
        print ('XXXXXXX',line)
        print ('XXXXXXX',e)
        continue

alldirs = list(datadict.keys())
alldirs.sort()
for clientproject in alldirs:
    print (clientproject[0],'\t',clientproject[1],'\t',datadict[clientproject][0],'\t',
           datadict[clientproject][1],'\t',datadict[clientproject][2])
#pprint.pprint (recentcount)
#pprint.pprint (totalcount)
    
