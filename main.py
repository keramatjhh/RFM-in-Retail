from flask import Flask,request
from flask import Response
from scripts.churn.gatherData import GetData
from scripts.churn.trainModels import Train
from scripts.churn.getResult import PredictChurners

from scripts.discount.Churn import ChurnBasketRecommend
from scripts.discount.Commodity import GetCommodity
from scripts.discount.CLV import CalculateCLV
from scripts.discount.FirstBuy import FirstBuySuggestion
from scripts.discount.CustomerSegmentation import SegmentationPredict,SegmentationTrainModel

import logging
from logging import handlers
from datetime import datetime

from os import path
app = Flask(__name__)
from threading import Lock

@app.route('/churn/refreshdata',methods=["GET"])
def RefreshData():

    if gather_lock.locked():
        logger.info("Gathering Data Locked.")
        return Response("در خواستی مشابه در حال پردازش است",status=400)
    else:
        gather_lock.acquire()

    DATE_STRING_FORMAT = "%Y-%m-%d"
    startdate = request.args.get('startdate')
    enddate = request.args.get('enddate')

    try:
        startdate = datetime.strptime(startdate,DATE_STRING_FORMAT)
        enddate = datetime.strptime(enddate,DATE_STRING_FORMAT)
        GetData(startdate,enddate)
        message_status = ("Success",200)
    except ValueError as e:
        logger.error(str(e),exc_info=True)
        message_status = ("از صحت فرمت تاریخ مطمئن شوید.",400)

    except TypeError as e:
        logger.error(str(e),exc_info=True)
        message_status = ("فیلد‌های enddate و startdate را وارد نمایید.", 400)

    except Exception as e:
        logger.error(str(e),exc_info=True)
        message_status = ("با ادمین سامانه تماس بگیرید.",500)

    finally:
        gather_lock.release()
        return Response(*message_status)

@app.route('/churn/trainmodel',methods=["GET"])
def TrainModel():
    if train_lock.locked():
        logger.info("Train Model Data Locked.")
        return Response("در خواستی مشابه در حال پردازش است",status=400)
    else: 
        train_lock.acquire()

    model = request.args.get('model')
    startdate = request.args.get('startdate')
    enddate = request.args.get('enddate')
    churn_interval = request.args.get('churn_interval')
    # monetary_limit = request.args.get('monetary_limit')

    DATE_STRING_FORMAT = "%Y-%m-%d"
    message_status = ("Success",200)
    try:
        startdate = datetime.strptime(startdate,DATE_STRING_FORMAT)
        enddate = datetime.strptime(enddate,DATE_STRING_FORMAT)
        churn_interval = int(churn_interval)
        # monetary_limit = int(monetary_limit)
        if model is None: raise TypeError()
        
        message_status = (Train(model,startdate,enddate,churn_interval),200)

    except ValueError as e:
        logger.error(str(e),exc_info=True)
        message_status = ("از صحت اطلاعات ورودی مطمئن شوید.",400)
    except TypeError as e:
        logger.error(str(e),exc_info=True)
        message_status = ("فیلد‌های enddate و startdate و churn_interval و model  را وارد نمایید.",400)
    except Exception as e:
        logger.error(str(e),exc_info=True)
        message_status = ("با ادمین سامانه تماس بگیرید.",500)
    finally:
        train_lock.release()
        return Response(*message_status)
    
@app.route('/churn/predict',methods=["GET"])    
def GetChurners():
    startdate = request.args.get('startdate')
    enddate = request.args.get('enddate')
    limit = request.args.get('limit')
    monetary_limit = request.args.get('monetary_limit')
#   add data limits
    DATE_STRING_FORMAT = "%Y-%m-%d"
    message_status = (None,200)

    try:
        startdate = datetime.strptime(startdate,DATE_STRING_FORMAT)
        enddate = datetime.strptime(enddate,DATE_STRING_FORMAT)

        limit = limit if limit is not None else 20
        limit = int(limit)

        if monetary_limit is None:
            pass
        else:
            monetary_limit = int(monetary_limit)


        # if model is None: raise TypeError()

        message_status = (PredictChurners(startdate,enddate,limit,monetary_limit),200)

    except ValueError as e:
        logger.error(str(e),exc_info=True)
        message_status = ("از صحت اطلاعات ورودی مطمئن شوید.",400)
    except TypeError as e:
        logger.error(str(e),exc_info=True)
        message_status = ("فیلد‌های enddate و startdate و limit و monetary-limit  را وارد نمایید.",400)
    except Exception as e:
        logger.error(str(e),exc_info=True)
        message_status = ("با ادمین سامانه تماس بگیرید.",500)
    finally:
        return Response(*message_status)

@app.route('/discount/churners',methods=["GET"])
def ChurnersDiscount():
    custID = request.args.get('custID')
    limit = request.args.get('limit')

    message_status = (None,200)
    try:
        limit = int(limit)
        if custID is None: raise TypeError()
        message_status= (ChurnBasketRecommend(custID,limit),200)
    except ValueError as e:
        logger.error(str(e),exc_info=True)
        message_status = ("از صحت اطلاعات ورودی مطمئن شوید.",400)
    except TypeError as e:
        logger.error(str(e),exc_info=True)
        message_status = ("فیلد‌های  و custID و limit  را وارد نمایید.",400)
    except Exception as e:
        logger.error(str(e),exc_info=True)
        message_status = ("با ادمین سامانه تماس بگیرید.",500)
    finally:
        return Response(*message_status)

@app.route('/discount/commodity',methods=["GET"])
def Commodity():
    limit = request.args.get('limit')
    startdate = request.args.get('startdate')


    message_status = (None,200)
    DATE_INPUT_STRING_FORMAT = "%Y-%m-%d"
    DATE_OUTPUT_STRING_FORMAT = "%Y/%m/%d"

    try:
        limit = int(limit)
        startdate = datetime.strptime(startdate,DATE_INPUT_STRING_FORMAT)
        startdate = startdate.strftime(DATE_OUTPUT_STRING_FORMAT)
        message_status= (GetCommodity(startdate,limit),200)
    except ValueError as e:
        logger.error(str(e),exc_info=True)
        message_status = ("از صحت اطلاعات ورودی مطمئن شوید.",400)
    except TypeError as e:
        logger.error(str(e),exc_info=True)
        message_status = ("فیلد‌های  و startdate و limit  را وارد نمایید.",400)
    except Exception as e:
        logger.error(str(e),exc_info=True)
        message_status = ("با ادمین سامانه تماس بگیرید.",500)
    finally:
        return Response(*message_status)

@app.route('/discount/clv',methods=["GET"])
def CLV():
    lifespan = request.args.get('lifespan')
    startdate = request.args.get('startdate')
    enddate = request.args.get('enddate')


    message_status = (None,200)
    DATE_INPUT_STRING_FORMAT = "%Y-%m-%d"
    DATE_OUTPUT_STRING_FORMAT = "%Y/%m/%d"

    try:
        lifespan = int(lifespan)

        enddate = datetime.strptime(enddate,DATE_INPUT_STRING_FORMAT)
        enddate = enddate.strftime(DATE_OUTPUT_STRING_FORMAT)

        startdate = datetime.strptime(startdate,DATE_INPUT_STRING_FORMAT)
        startdate = startdate.strftime(DATE_OUTPUT_STRING_FORMAT)

        message_status= (CalculateCLV(lifespan,startdate,enddate),200)
    except ValueError as e:
        logger.error(str(e),exc_info=True)
        message_status = ("از صحت اطلاعات ورودی مطمئن شوید.",400)
    except TypeError as e:
        logger.error(str(e),exc_info=True)
        message_status = ("فیلد‌های  و startdate و lifespan و enddate  را وارد نمایید.",400)
    except Exception as e:
        logger.error(str(e),exc_info=True)
        message_status = ("با ادمین سامانه تماس بگیرید.",500)
    finally:
        return Response(*message_status)

@app.route('/discount/firstbuy',methods=["GET"])
def FirstBuy():
    custId = request.args.get('custId')
    startdate = request.args.get('startdate')
    enddate = request.args.get('enddate')


    message_status = (None,200)
    DATE_INPUT_STRING_FORMAT = "%Y-%m-%d"
    DATE_OUTPUT_STRING_FORMAT = "%Y/%m/%d"

    try:
        if custId is None: raise TypeError()

        enddate = datetime.strptime(enddate,DATE_INPUT_STRING_FORMAT)
        enddate = enddate.strftime(DATE_OUTPUT_STRING_FORMAT)

        startdate = datetime.strptime(startdate,DATE_INPUT_STRING_FORMAT)
        startdate = startdate.strftime(DATE_OUTPUT_STRING_FORMAT)

        message_status= (FirstBuySuggestion(custId,startdate,enddate),200)
    except ValueError as e:
        logger.error(str(e),exc_info=True)
        message_status = ("از صحت اطلاعات ورودی مطمئن شوید.",400)
    except TypeError as e:
        logger.error(str(e),exc_info=True)
        message_status = ("فیلد‌های  و startdate و custId و enddate  را وارد نمایید.",400)
    except Exception as e:
        logger.error(str(e),exc_info=True)
        message_status = ("با ادمین سامانه تماس بگیرید.",500)
    finally:
        return Response(*message_status)
 
@app.route('/discount/customersegmentation/train',methods=["GET"])
def TrainCustomerSegmentation():
    startdate = request.args.get('startdate')
    enddate = request.args.get('enddate')
    k = request.args.get('k')
    show_elbow = request.args.get('show_elbow')

    message_status = (None,200)
    DATE_INPUT_STRING_FORMAT = "%Y-%m-%d"

    try:
        
        if k is None:
            k = 4
        else:
            k = int(k)
        
        enddate = datetime.strptime(enddate,DATE_INPUT_STRING_FORMAT)

        startdate = datetime.strptime(startdate,DATE_INPUT_STRING_FORMAT)
        show_elbow= show_elbow.lower()
        if show_elbow == 'false':
            show_elbow = False
        elif show_elbow== 'true':
            show_elbow = True
        else:
            raise ValueError

        message_status= (SegmentationTrainModel(startdate,enddate,k,show_elbow),200)

    except ValueError as e:
        logger.error(str(e),exc_info=True)
        message_status = ("از صحت اطلاعات ورودی مطمئن شوید.",400)
    except TypeError as e:
        logger.error(str(e),exc_info=True)
        message_status = ("فیلد‌های  و startdate و custId و enddate  را وارد نمایید.",400)
    except Exception as e:
        logger.error(str(e),exc_info=True)
        message_status = ("با ادمین سامانه تماس بگیرید.",500)
    finally:
        return Response(*message_status)


@app.route('/discount/customersegmentation/predict',methods=["GET"])
def PredictCustomerSegmentation():
    custId = request.args.get('custId')

    message_status = (None,200)

    try:
        if custId is None: raise TypeError()

        message_status= (SegmentationPredict(custId),200)
    except ValueError as e:
        logger.error(str(e),exc_info=True)
        message_status = ("از صحت اطلاعات ورودی مطمئن شوید.",400)
    except TypeError as e:
        logger.error(str(e),exc_info=True)
        message_status = ("فیلد‌های  و startdate و custId و enddate  را وارد نمایید.",400)
    except Exception as e:
        logger.error(str(e),exc_info=True)
        message_status = ("با ادمین سامانه تماس بگیرید.",500)
    finally:
        return Response(*message_status)
 
if __name__ == "__main__":

    logger = logging.getLogger()
    
    fh = handlers.TimedRotatingFileHandler(path.join("App","logs","Etka-AI.log"),when='midnight')
    formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | [%(filename)20s:%(lineno)04d] | %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    logger.setLevel(logging.DEBUG)

    gather_lock = Lock()
    train_lock = Lock()
    app.run(debug=True)