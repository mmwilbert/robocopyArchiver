import re
import csv
from pprint import pformat,pprint
import datetime
import sys
import csv
import traceback
import logging

#newFileRe = re.compile(r"\\s+([^\s][0-9\s\.mg]+)\s(?:[^\n\r]+)")
newFileRe = re.compile(r"\s+New File\s+([[0-9\s\.mg]+)\s([^\n\r]+)")
#newFileRe = re.compile(r"\s+New File\s+([^\s].*)")
#errorRe = re.compile(r"([0-9/\s\:]\sERROR ([0-9]+)\s(.*))$")
ignoreRe = re.compile(r"""(?:[\s]+$)|  #blank
                        (?:\-+[\r]$)| #hyphens
                        (?:\s+ROBOCOPY\s+\:\:.*$)| #ROBOCOPY header
                        (?:\s+Total\s+Copied\s+Skipped.*$)  #totals header
                        """,re.X)
extraDirRe = re.compile(r"\s+\*EXTRA Dir\s+([^\s][0-9\s\.mg]+)\s([^\n\r]+)")
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
    return d.strftime("%Y-%m-%d %H:%M:%S")

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
        self.unrecognizedLineList = []

        self.fileCsvWriter = csv.DictWriter(filecsv,self.fileCsvFields,restval="",lineterminator='\n')
        self.fileCsvWriter.writeheader()
        self.metaCsvWriter = csv.DictWriter(metacsv,self.metaCsvFields,restval="",lineterminator='n')
        self.metaCsvWriter.writeheader()
        
        
        self.robocopyRecordReAndHandlerList = [
            (newFileRe,self.newFileHandler),
            (errorRe,self.errorReHandler),
            (extraDirRe,self.extraDirReHandler),
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
        logging.debug("in blankReHandler")
        if self.currentFileRecordDict:
            self.writeFileRecord()

    def errorReHandler(self,matchObject):
        logging.debug("in errorReHandler")
        self.currentFileRecordDict['ErrorString'] = matchObject.group(1).strip()
        self.currentFileRecordDict['ErrorCode'] = matchObject.group(2).strip()
        logging.debug("file record:" + pformat(self.currentFileRecordDict))
        return
    
    def extraDirReHandler(self,matchObject):
        logging.debug("extradir found")
        if self.currentFileRecordDict:
            self.writeFileRecord()
        self.currentFileRecordDict['Filename'] = filename
        self.currentFileRecordDict['Type'] = "ExtraDir"
        self.currentFileRecordDict['Size'] = getSizeFromRobosize(matchObject.group(1).strip())
        self.currentFileRecordDict['BackupStartDate'] = self.backupMetadataDict['Started']
        
    def ignoreReHandler(self,matchObject):
        logging.debug("ignored line found")

    def dividerReHandler(self,matchObject):
        logging.debug("divider found")
        if self.currentFileRecordDict:
            self.writeFileRecord()

    def startedReHandler(self,matchObject):
        logging.debug ("started found")
        self.backupMetadataDict['Started'] = convertRobocopyDateToBQFormat(matchObject.group(1))
    def sourceReHandler(self,matchObject):
        logging.debug("source found")
        self.backupMetadataDict['Source'] = matchObject.group(1).strip()

    def destReHandler(self,matchObject):
        logging.debug ("dest found")
        self.backupMetadataDict['Dest'] = matchObject.group(1).strip()

    def filesReHandler(self,matchObject):
        logging.debug ("files found")
        self.backupMetadataDict['Files'] = matchObject.group(1).strip()

    def optionsReHandler(self,matchObject):
        logging.debug ("options handler")
        self.backupMetadataDict['Options'] = matchObject.group(1).strip()

    def dirCountReHandler(self,matchObject):
        logging.debug("dircount handler")
        self.backupMetadataDict['dirstotal'] = getSizeFromRobosize(matchObject.group(1))
        self.backupMetadataDict['dirscopied'] = getSizeFromRobosize(matchObject.group(2))
        self.backupMetadataDict['dirsskipped'] = getSizeFromRobosize(matchObject.group(3))
        self.backupMetadataDict['dirsmismatch'] = getSizeFromRobosize(matchObject.group(4))
        self.backupMetadataDict['dirsfailed'] = getSizeFromRobosize(matchObject.group(5))
        self.backupMetadataDict['dirsextras'] = getSizeFromRobosize(matchObject.group(6))

    def fileCountReHandler(self,matchObject):
        logging.debug("filecount handler")
        self.backupMetadataDict['filestotal'] = getSizeFromRobosize(matchObject.group(1))
        self.backupMetadataDict['filescopied'] = getSizeFromRobosize(matchObject.group(2))
        self.backupMetadataDict['filesskipped'] = getSizeFromRobosize(matchObject.group(3))
        self.backupMetadataDict['filesmismatch'] = getSizeFromRobosize(matchObject.group(4))
        self.backupMetadataDict['filesfailed'] = getSizeFromRobosize(matchObject.group(5))
        self.backupMetadataDict['filesextras'] = getSizeFromRobosize(matchObject.group(6))

    def byteCountReHandler(self,matchObject):
        logging.debug("bytecount handler")
        self.backupMetadataDict['bytestotal'] = getSizeFromRobosize(matchObject.group(1))
        self.backupMetadataDict['bytescopied'] = getSizeFromRobosize(matchObject.group(2))
        self.backupMetadataDict['bytesskipped'] = getSizeFromRobosize(matchObject.group(3))
        self.backupMetadataDict['bytesmismatch'] = getSizeFromRobosize(matchObject.group(4))
        self.backupMetadataDict['bytesfailed'] = getSizeFromRobosize(matchObject.group(5))
        self.backupMetadataDict['bytesextras'] = getSizeFromRobosize(matchObject.group(6))

    def timesReHandler(self,matchObject):
        logging.debug ("times handler")
        self.backupMetadataDict['timestotal'] = matchObject.group(1).strip()
        self.backupMetadataDict['timescopied'] = matchObject.group(1).strip()
        self.backupMetadataDict['timesfailed'] = matchObject.group(1).strip()
        self.backupMetadataDict['timesextras'] = matchObject.group(1).strip()

    def speedBytesReHandler(self,matchObject):
        logging.debug("speed bytes handler")
        self.backupMetadataDict['speedBytes'] = matchObject.group(1).strip()

    def speedMegabytesReHandler(self,matchObject):
        logging.debug ("speed mega handler")
        self.backupMetadataDict['speedMegaBytes'] = matchObject.group(1).strip()

    def endedReHandler(self,matchObject):
        logging.debug ("ended handler")
        if self.currentFileRecordDict:
            self.writeFileRecord()
        self.backupMetadataDict['Ended'] = convertRobocopyDateToBQFormat(matchObject.group(1))
        self.writeBackupMetadata()

    def newFileHandler(self,matchObject):
        if self.currentFileRecordDict:
            self.writeFileRecord()
        filename = matchObject.group(2).strip()
        if filename[-11:] == "Retrying...":
            self.currentFileRecordDict['Filename'] = filename[:-12]
            self.currentFileRecordDict['Type'] = "Retry"
        else:
            self.currentFileRecordDict['Filename'] = filename
            self.currentFileRecordDict['Type'] = "New"
        self.currentFileRecordDict['Size'] = getSizeFromRobosize(matchObject.group(1).strip())
        self.currentFileRecordDict['BackupStartDate'] = self.backupMetadataDict['Started']
        logging.debug(self.currentFileRecordDict)
    

    def processLine(self,line):
        logging.debug("entering processLine")
        logging.debug("line: " + line)
        for lineRe,reHandler in rlc.robocopyRecordReAndHandlerList:
            rlc.lineMatched = False
            matchObject = lineRe.match(line)
            if matchObject:
                rlc.lineMatched = True
                reHandler(matchObject)
                break
        logging.debug("done with handlers")
        if not rlc.lineMatched:
            if rlc.currentFileRecordDict.get('ErrorCode'):
                logging.debug("setting ErrorString")
                if rlc.currentFileRecordDict.get('Error'):
                    rlc.currentFileRecordDict['Error'] = rlc.currentFileRecordDict['Error'] + line.strip()
                else:
                    rlc.currentFileRecordDict['Error'] = line.strip()
            else:
                #if we get here, we have an unrecognized line, need to flag that
                rlc.unrecognizedLineList.append(line)
                logging.debug("No match:" + line)
            logging.debug(pformat(rlc.currentFileRecordDict))

        
    def writeFileRecord(self):
        logging.debug("writing file record")
        stripDictStrings(self.currentFileRecordDict)
        self.currentFileRecordDict['BackupStartDate'] = self.backupMetadataDict['Started']
        logging.debug(pformat(self.currentFileRecordDict))
        self.fileCsvWriter.writerow(self.currentFileRecordDict)
        self.currentFileRecordDict = {}

    def writeBackupMetadata(self):
        stripDictStrings(self.backupMetadataDict)
        self.metaCsvWriter.writerow(self.backupMetadataDict)
        


filename = sys.argv[1]
filecsvname = filename + "_files.csv"
metacsvname = filename + "_meta.csv"
#logging.basicConfig(level=logging.DEBUG)

try:
    with open(filename,'r',encoding='UTF-8',errors='ignore',newline='\n') as robolog, \
         open(filecsvname,'w') as filecsv, \
         open(metacsvname,'w') as metacsv:
        rlc = robocopyLogConverter(filecsv,metacsv)

        filerecord = ""
        for line in robolog:
            rlc.processLine(line)
        if rlc.unrecognizedLineList:
            print("there were unrecognized lines")
            pprint(rlc.unrecognizedLineList)
        sys.exit(4)
except:
    print("exception")
    print(traceback.format_exc())
    print(line)
    print(pformat(rlc.unrecognizedLineList))
    sys.exit(2)
    
