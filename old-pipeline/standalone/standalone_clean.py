"""
Run the clean task stand alone for debugging
"""
import datetime
from clean import clean

#casalog.filter('DEBUGGING')
sp = "/mnt/output/Chiles/split_vis"
vf = "vis_1136~1140"

start_time = datetime.datetime.now()
clean(vis=['{0}/20131116-946-6/{1}'.format(sp, vf),'{0}/20131117-941-6/{1}'.format(sp, vf),
           '{0}/20131118-946-6/{1}'.format(sp, vf),'{0}/20131119-941-6/{1}'.format(sp, vf),
           '{0}/20131121-946-6/{1}'.format(sp, vf),'{0}/20131123-951-6/{1}'.format(sp, vf),
           '{0}/20131126-946-6/{1}'.format(sp, vf),'{0}/20131203-941-6/{1}'.format(sp, vf)],
      imagename="/mnt/output/Chiles/cube_1136~1140",
      outlierfile="",field="deepfield",spw="",selectdata=True,timerange="",uvrange="",antenna="",scan="",observation="",intent="",mode="frequency",resmooth=False,gridmode="",
      wprojplanes=1,facets=1,cfcache="cfcache.dir",rotpainc=5.0,painc=360.0,aterm=True,psterm=False,mterm=True,wbawp=False,conjbeams=True,epjtable="",interpolation="nearest",
      niter=0,gain=0.1,threshold="0.0mJy",psfmode="clark",imagermode="csclean",ftmachine="mosaic",mosweight=False,scaletype="SAULT",multiscale=[0],negcomponent=-1,
      smallscalebias=0.6,interactive=False,mask=[],nchan=-1,start="",width="",outframe="BARY",veltype="optical",imsize=[2048],cell=['1.5arcsec'],phasecenter="",
      restfreq="1420.405752MHz",stokes="I",weighting="natural",robust=0.0,uvtaper=False,outertaper=[''],innertaper=['1.0'],modelimage="",restoringbeam=[''],pbcor=False,
      minpb=0.2,usescratch=True,noise="1.0Jy",npixels=0,npercycle=100,cyclefactor=1.5,cyclespeedup=-1,nterms=1,reffreq="",chaniter=False,flatnoise=True,allowchunk=False)

end_time = datetime.datetime.now()
print 'Time taken:', end_time, start_time, end_time - start_time
