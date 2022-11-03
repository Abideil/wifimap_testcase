# wifimap_testcase

To run this containeer you have to write in your terminal:

docker build -t flask-app . 

and after that:

sudo docker run -it -p 5000:5000 -d flask-app  

Then you can open http://localhost:5000

The project is not fully ready: clicking on the buttons causes to the output of datasets in the log, visualization is also not ready.


nov 3 - Update 1 - Added dataset display and visualisation, but not at time.
