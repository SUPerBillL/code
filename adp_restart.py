#!/user/local/python/bin/python
# coding=utf-8
# /***************************************************************************************************/
# /*Script Name: adp_restart.py           	 												         */
# /*Create Date: 2021-01-01                                                                          */
# /*Script Developed By: ssq                                                                         */
# /*Script Use : checkLoad temlate                                                                   */
# /*******************************Import SysModules***************************************************/
import sys
import os
import traceback
import time
# /*******************************environ Parameters**************************************************/
envDist = os.environ
_AUTOHOME = envDist.get('AUTO_HOME')                #获取系统变量AUTO_HOME
_APPHOME = envDist.get('APP_HOME')                  #获取系统变量APP_HOME
_AOTOETLDB = envDist.get('AUTO_ETL_DB')             #获取系统变量AUTO_ETL_DB
sys.path.insert(0, '%s/ETLSuite' % (_APPHOME))
sys.path.insert(0, '%s/jobBase' % (_APPHOME))
# /*******************************Import CustomModules*************************************************/
import tools.MysqlConn as coon
from job.base.LogUtil import LogUtil
from tools.ShellUtil import *

if sys.getdefaultencoding() != 'utf-8':
    reload(sys)
    sys.setdefaultencoding('utf-8')

# /*******************************SYS ARGVS **********************************************************/
_RESTART_ID = sys.argv[1]                               # 重调id
_RESTART_LOG = sys.argv[2]                              # 日志文件目录
# /*******************************Defined ARGVS ******************************************************/
shellCode = 'RESATRT'                                  # 脚本标识
RESTART_TYPE_SCHEDU = 'SingleSchedule'                 #调度单独重调
RESTART_TYPE_SCRIPT = ['SingleScript','MutiScript']   #脚本瀑布重调
DONE_STATUS = 'Done'                                   #补数作业状态(成功)
RUNNING_STATUS = 'Running'                             #补数作业状态(运行中)
FAILED_STATUS = 'Failed'                               #补数作业状态(失败)
_RESTART_STARTUS=DONE_STATUS                           #初始化重调任务的默认值
_RECEIVEPATH = '%s/DATA/receive' % (_AUTOHOME)        #调度信号文件目录
# /*******************************Init  ARGVS*********************************************************/
_ETL_JOB = ''                                          #运行的作业名
_BIZDATE = ''                                          #运行的业务日期
_SYSTEM = ''                                           #运行的系统名
_LOGFILE = _RESTART_LOG + shellCode + os.sep + "DEFAULT" \
           + os.sep +shellCode+'.log'                  #默认日志路径



# 日志打印
def log(message, format=True):
    if format:
        LogUtil._LOG_FILE = _LOGFILE
        LogUtil.info(str(message))
    else:
        print str(message)
        fs = open(_LOGFILE, 'a')
        fs.write(str(message) + '\n')
        fs.close()

def errlog(message):
    LogUtil._LOG_FILE = _LOGFILE
    LogUtil.error(str(message))


# 获取当前系统时间
def curentTime():
    return (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

def updeleTableStatus(etlTbName,etlTxDate):
    global _AOTOETLDB
    sql = "UPDATE %s.ETL_JOB " \
          "SET LAST_JOBSTATUS = 'Ready'" \
          " WHERE ETL_JOB = '%s' " \
          "   AND LAST_TXDATE = STR_TO_DATE('%s', '%%Y%%m%%d')"  % (_AOTOETLDB,etlTbName,etlTxDate)
    result = coon.execSql(sql)
    if coon.isError() or int(result) <> 1:
        raise Exception('重调作业失败:%s' %(etlTbName))
    sql = "DELETE FROM %s.ETL_JOB_STATUS" \
          " WHERE ETL_JOB = '%s' AND TXDATE = STR_TO_DATE(%s, '%%Y%%m%%d')" % (_AOTOETLDB, etlTbName, etlTxDate)
    coon.execSql(sql)
    if coon.isError():
        raise Exception('二次重跑删除作业状态失败!')
    sql = "COMMIT"
    coon.execSql(sql)


def createFile():
    _receiveName = "dir." + str(_ETL_JOB).lower() + str(_BIZDATE)
    filename1 = _RECEIVEPATH + os.sep + _receiveName
    ret = False
    log("开始建立信号文件"+str(filename1))
    if not (os.path.exists(filename1)):  # 在receive文件下建立文件，触发调度
        open(filename1, 'w+')
        message= str(_receiveName) + '，建立文件成功'
        print str(message)
        log(message)
        ret = True
    return ret

def getDetilInfo():
    sql = "   SELECT TRIM(T1.ID)," \
          "          TRIM(T1.ETL_SYSTEM)," \
          "          TRIM(T1.ETL_JOB)," \
          "          DATE_FORMAT(T1.TXDATE,'%%Y%%m%%d')," \
          "          TRIM(T2.SCRIPT_NAME)" \
          "     FROM %s.ETL_JOB_RESTART_DETAIL T1 " \
          "LEFT JOIN %s.ETL_JOB_SCRIPT T2" \
          "       ON T1.ETL_SYSTEM=T2.ETL_SYSTEM" \
          "      AND T1.ETL_JOB=T2.ETL_JOB" \
          "    WHERE T1.RESTART_ID = '%s' " \
          "    ORDER BY RESTART_INDEX ASC"  % (_AOTOETLDB,_AOTOETLDB,_RESTART_ID)
    result = coon.execSql(sql)
    if coon.isError() or result == None:
        raise Exception('获取脚本信息失败:%s' %(_RESTART_ID))
    return result

def getSchDetilInfo():
    global _ETL_JOB, _BIZDATE, _SYSTEM
    id=''
    sql = "   SELECT TRIM(T1.ID)," \
          "          TRIM(T1.ETL_SYSTEM)," \
          "          TRIM(T1.ETL_JOB)," \
          "          DATE_FORMAT(T1.TXDATE,'%%Y%%m%%d')" \
          "     FROM %s.ETL_JOB_RESTART_DETAIL T1 " \
          "    WHERE T1.RESTART_ID = '%s' " % (_AOTOETLDB,_RESTART_ID)
    result = coon.execSql(sql)
    if coon.isError() or result == None:
        raise Exception('获取明细信息失败:%s' %(_RESTART_ID))
    result=list(result)
    while result:
        detilInfo = result.pop(0)
        id = detilInfo[0]
        _SYSTEM = detilInfo[1]
        _ETL_JOB = detilInfo[2]
        _BIZDATE = detilInfo[3]
    if id =='':
        raise Exception('获取明细信息失败:%s' % (id))
    return id


def getEtlJobStatus():
    sql = "   SELECT TRIM(T1.JOBSTATUS)" \
          "     FROM %s.ETL_JOB_STATUS T1 " \
          "    WHERE T1.ETL_JOB = '%s' " \
          "      AND T1.ETL_SYSTEM = '%s'" \
          "      AND T1.TXDATE = '%s' "% (_AOTOETLDB,_ETL_JOB,_SYSTEM,_BIZDATE)
    result = coon.execSql(sql)
    if coon.isError():
        raise Exception('获取作业状态失败:%s' %(_ETL_JOB))
    return result

def getRestartInfo():
    sql = "   SELECT TRIM(T1.RESTART_TYPE)" \
          "     FROM %s.ETL_JOB_RESTART T1 " \
          "    WHERE T1.ID = '%s' "  % (_AOTOETLDB,_RESTART_ID)
    result = coon.execSql(sql)
    if coon.isError():
        raise Exception('获取重调信息失败:%s' %(_RESTART_ID))
    return result[0]

def updateRestartStatus(status):
    sql = "UPDATE %s.ETL_JOB_RESTART T1" \
          "   SET T1.STATUS = '%s', " \
          "       T1.END_TIME = '%s'," \
          "       T1.PROCESS_ID=NULL" \
          " WHERE T1.ID = '%s'"  % (_AOTOETLDB,status,curentTime(),_RESTART_ID)
    result = coon.execSql(sql)
    if coon.isError() or int(result) <> 1:
        raise Exception('获取脚本信息失败:%s' %(_RESTART_ID))

def updateDeStatus(id,status):
    if _LOGFILE != None:
        logSql="       LOG_PATH = '%s'" % _LOGFILE
    else:
        logSql="       LOG_PATH = Null" 
    sql = "UPDATE %s.ETL_JOB_RESTART_DETAIL T1" \
          "   SET JOB_STATUS = '%s', " \
          "   %s WHERE T1.ID = '%s'"  % (_AOTOETLDB,status,logSql,id)
    result = coon.execSql(sql)
    if coon.isError() or int(result) <> 1:
        raise Exception('修改作业状态失败:%s' %(id))
    sql = "COMMIT;"
    coon.execSql(sql)

def init():
    global _LOGFILE
    #初始化日志
    LOGPATH = _RESTART_LOG + shellCode + os.sep + _RESTART_ID + os.sep + _BIZDATE + os.sep  # 默认日志文件地址
    _LOGFILE = LOGPATH + _ETL_JOB + '.log'  # 日志文件

    if not os.path.exists(LOGPATH):
        os.makedirs(LOGPATH, 0755)

    if os.path.exists(_LOGFILE):
        os.remove(_LOGFILE)

#调起脚本
def invokeScript(id,etlJob,script,txdate,system):
    global _RESTART_STARTUS
    init()
    status = RUNNING_STATUS
    shellCmd = "sh %s %s %s %s" %(script,etlJob,txdate,system)
    message = '开始执行:%s' % (shellCmd)
    log(message)
    updateDeStatus(id,status)
    (result,shellLog)= ShellUtil.exShell2(shellCmd)
    if not result == 0 :
        _RESTART_STARTUS= FAILED_STATUS
        status = FAILED_STATUS 
    else:
        status = DONE_STATUS
    log(shellLog)
    updateDeStatus(id,status)
    return status

#补数入口方法
def complement():
    global _ETL_JOB, _BIZDATE, _SYSTEM, _LOGFILE
    detilInfos = list(getDetilInfo())
    i = 0
    try:
        while detilInfos:

            detilInfo = detilInfos.pop(0)
            id = detilInfo[0]
            _SYSTEM = detilInfo[2]
            _ETL_JOB = detilInfo[2]
            _BIZDATE = detilInfo[3]
            script = detilInfo[4]
            if i == 0:
                status = invokeScript(id,_ETL_JOB,script,_BIZDATE,_SYSTEM)
                if status == FAILED_STATUS:
                    i += 1
            else:
                _LOGFILE=None
                updateDeStatus(id, FAILED_STATUS)
    except Exception as e:
        errInfo = str(_ETL_JOB) + "作业补数失败，错误原因:\n%s" % traceback.format_exc()
        errlog(errInfo)



#重调入口方法
def retuning():
    global _LOGFILE
    result=True
    init()
    id = getSchDetilInfo()
    updeleTableStatus(_ETL_JOB, _BIZDATE)
    if not (createFile()):
        raise Exception('创建信号文件失败:%s' % (_RESTART_ID))
    while result:
        log("开始检查作业状态")
        sth = getEtlJobStatus()
        if len(sth) > 0 :
            status = sth[0][0]
            result = False
            _LOGFILE = ''
            updateDeStatus(id,status)
        time.sleep(10)


def main():
    global _ETL_JOB,_BIZDATE
    try:
        sth = getRestartInfo()
        type = sth[0]
        if type==RESTART_TYPE_SCHEDU:
            retuning()
        elif type in RESTART_TYPE_SCRIPT:
            complement()
        else:
                raise Exception('重调类型错误:%s' % (type))
        updateRestartStatus(_RESTART_STARTUS)
    except Exception as e:
        updateRestartStatus(FAILED_STATUS)
        errInfo = "重调或补数异常，错误原因:\n%s" % traceback.format_exc()
        errlog(errInfo)


# 主函数
if __name__ == "__main__":
    main()




