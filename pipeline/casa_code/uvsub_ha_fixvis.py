#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#
"""
Perform the UV Subtraction
"""
import logging
import os
import numpy as np
import sys
if os.path.exists(os.getenv('HOME')+'/aws-chiles02/pipeline/analysis_scripts'):
  sys.path.append(os.getenv('HOME')+'/aws-chiles02/pipeline/analysis_scripts')
elif os.path.exists(os.getenv('HOME')+'/Git/aws-chiles02/pipeline/analysis_scripts'):
  sys.path.append(os.getenv('HOME')+'/Git/aws-chiles02/pipeline/analysis_scripts')
import analysisUtils as au

from casa_code.casa_common import parse_args
from casa_code.echo import echo

casalog.filter('DEBUGGING')
logging.info('Starting logger for...')
LOG = logging.getLogger('uvsub')


# Define a proc for time conversion
@echo
def time_convert(mytime, myunit='s'):
    if type(mytime).__name__ != 'list':
        mytime = [mytime]
    my_timestr = []
    for time in mytime:
        q1 = qa.quantity(time, myunit)
        time1 = qa.time(q1, form='ymd')
        my_timestr.append(time1)
    return my_timestr


def do_uvsub(in_dir, out_dir, out_ms, out_pngs, w_projection_planes, number_taylor_terms, model):
    """
    Performs the UVSUB step
     Copy of uvsub.py adding in HA dependence of Outliers
     Assumes HA models are in the subdirectory of the Outlier models
     i.e. LSM/Outliers/HA_*/Out*model (as opposed to LSM/Outliers/Out*model)
     That these are 1/2 hour steps is hard wired (i.e. HA_ -16 to 16, representing HA -8.0 to 8.0 in 0.5 steps)
     Uses imtool to fill MODEL_DATA correctly
     runs UVSUB to fill CORRECTED_DATA (=DATA-MODEL_DATA)
     make uvsub_XX from vis_XX and column CORRECTED_DATA
    Inputs: VIS_name (str), MODEL (list of str)
    Number of taylor terms is length of model list. Expecting 2

    example:
     do_uvsub('vis_1400~1404',['Epoch1_Images_Wproject/epoch1.bb.gt4k.si_spw_14.model.tt0','Epoch1_Images_Wproject/epoch1.bb.gt4k.si_spw_14.model.tt1'])


     I suggest that SPW in the model strings is deduced from
     spw=int(((freq_min+freq_max)/2-946)/32)
    """
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    out_rot_data = 'no' # Keep a version of the subtracted data
    sub_uzero=True # False #or True
    if out_pngs == 'yes':
        png_directory = os.path.join(out_dir, 'qa_pngs')
        if not os.path.exists(png_directory):
            os.makedirs(png_directory)

    LOG.info(
        'uvsub(vis={0}, model={1}, out_dir={2}, out_ms={3}, w_projection_planes={4})'.format(
            in_dir,
            str(model),
            out_dir,
            out_ms,
            w_projection_planes))
    try:
        # dump_all()
        #
        #  Here for reference; not needed as no CD column
        # tb.open(vis_in_dir,nomodify=False)
        # tb.removecols('CORRECTED_DATA')
        # tb.close()
        #
        # Create/Flush model_data column
        im.open(thems=in_dir, usescratch=True)

        # Select all data in this case
        im.selectvis()

        # These are the parameters for the generation of the model
        # Not sure how many of them are important here -- all except mode?
        im.defineimage(
            nx=4096,
            ny=4096,
            cellx='2arcsec',
            celly='2arcsec',
            mode='mfs',
            facets=1
        )
        im.setoptions(ftmachine='wproject', wprojplanes=w_projection_planes)

        # Find the refernce frequency and set no. taylor terms
        ms.open(in_dir)
        fq = ms.getspectralwindowinfo()['0']['RefFreq']
        ms.close()

        # Special steps for outliers
        ntt = len(model)
        if ntt < number_taylor_terms:
            print 'Requested number of taylor terms: '+str(number_taylor_terms)
            print 'Is less than number of models given: '+str(ntt)
            print 'Setting former to the latter.'
            number_taylor_terms = ntt
        elif ntt > number_taylor_terms:
            tmp_name = os.path.join(out_dir, out_ms+'.tmp')
            ntt = number_taylor_terms
        print str(len(model))+' models provided. Using '+str(ntt)+' for spectral index subtraction'

        if ntt>0: # In-beam models
            im.settaylorterms(ntaylorterms=ntt, reffreq=fq)
            # Using this turned all data to NaNs -- not good!
            # im.settaylorterms(ntaylorterms=ntt)

            #
            print 'Models in this pass: '+str(model[0:ntt])
            for mn in model[0:ntt]:
              tb.open(mn)
              tb.clearlocks()
              tb.close()
            #
            im.ft(model=model[0:ntt], incremental=False)
            im.close()

            # Now do the subtraction
            uvsub(vis=in_dir, reverse=False)
            if out_pngs == 'yes':
              ret_d=plotms(vis=in_dir,xaxis='freq',yaxis='real',avgtime='43200',overwrite=True,avgbaseline=True,showgui=False,ydatacolumn='data',xdatacolumn='data',plotfile=png_directory+'/'+in_dir.rsplit('/')[-1]+'_infield_subtraction_data.png')
              ret_m=plotms(vis=in_dir,xaxis='freq',yaxis='real',avgtime='43200',overwrite=True,avgbaseline=True,showgui=False,ydatacolumn='model',xdatacolumn='model',plotfile=png_directory+'/'+in_dir.rsplit('/')[-1]+'_infield_subtraction_model.png')
              ret_c=plotms(vis=in_dir,xaxis='freq',yaxis='real',avgtime='43200',overwrite=True,avgbaseline=True,showgui=False,ydatacolumn='corrected',xdatacolumn='corrected',plotfile=png_directory+'/'+in_dir.rsplit('/')[-1]+'_infield_subtraction_corrected.png')
              if ((ret_d&ret_c&ret_m)==False):
                  print 'Reporting In-field PlotMS Failure! State for Data, Corrected and Model is: '+str(ret_d)+'&'+str(ret_c)+'&'+str(ret_m)
        else: # No in-beam models
            tmp_name = in_dir 
            #os.path.join(out_dir, out_ms)
        # End ntt>0

        # Do we have outliers??
        if len(model) > ntt:
            print 'Using remaing '+str(len(model)-ntt)+' for outlier subtraction'
            ha_model = []
            for m in range(len(model)):
                ha_model.append(model[m].replace('Outliers/','Outliers/HA_0/'))
            ms.open(thems=in_dir)
            # Select data by HA in this case
            ret = ms.getdata(['axis_info', 'ha'], ifraxis=True)
            ms_phasecentre=au.direction2radecForSimobserve(ms.getfielddirmeas())
            ms.close()
            ha = ret['axis_info']['time_axis']['HA']/3600.0
            print 'HA Range: '+str(ha[0])+' to '+str(ha[-1])
            ut = np.mod(ret['axis_info']['time_axis']['MJDseconds'] / 3600.0 / 24.0,1)*24.0
            not_first = False
            for nmodel in range(ntt,len(model)): #(ntt,len(model)):
                if ((ntt>0) & (not_first==False)): # in-beam model done
                    split(vis=in_dir, outputvis=tmp_name, datacolumn='corrected')
                ia.open(ha_model[nmodel])
                model_pc=ia.coordmeasures(ia.coordsys().referencepixel()['numeric'])['measure']['direction']
                model_pc=au.direction2radecForSimobserve(model_pc)
                ia.close()
                # alternating temporary names
                tmp_name1='%s.%d'%(tmp_name,0)
                tmp_name2='%s.%d'%(tmp_name,1)
                os.system('rm -r %s'%(tmp_name2))
                #if nmodel==(ntt): # First
                #    tmp_name1=tmp_name
                # Rotate to the direction of ha_model[nmodel]
                fixvis(vis=tmp_name,outputvis=tmp_name2,phasecenter=model_pc)
                #tmp_name1='%s.%d'%(tmp_name,0)
                im.open(thems=tmp_name2, usescratch=True)
                # delmod(otf=True,vis=tmp_name,scr=True)
                for m in range(-16, 16):
                    ptr1 = np.where(ha > (0.5*m))[0]
                    ptr2 = np.where(ha > (0.5*(m+1)))[0]
                    print 'No. samples in this HA range: '+str(len(ptr1)-len(ptr2))
                    if (len(ptr1) != 0) & (len(ptr1) != len(ptr2)):
                        print 'Change Model to point to directory: '+'HA_'+str(m)
                        ha_model[nmodel] = model[nmodel].replace('Outliers', 'Outliers/HA_'+str(m))
                        ptr = ptr1[0]
                        print 'This HA ('+str(m*0.5)+') will start at '+str(ptr)+' and use the following adjusted model: '+str(ha_model[nmodel])
                        # ut_start=ut[ptr]
                        date_start = time_convert(ret['axis_info']['time_axis']['MJDseconds'][ptr])[0][0]
                        print 'to start at '+date_start
                        if len(ptr2) == 0:
                            ptr = -1
                        else:
                            ptr = ptr2[0]
                        # ut_end=ut[ptr]
                        date_end = time_convert(ret['axis_info']['time_axis']['MJDseconds'][ptr])[0][0]
                        print 'and to end at '+date_end+' sample no. '+str(ptr)
                        # if (ut_end<ut_start):
                        #    ut_end=ut_end+24
                        # timerange=str(ut_start)+'~'+str(ut_end)
                        timerange = date_start + '~' + date_end
                        im.selectvis(time=timerange)
                        # These are the parameters for the generation of the model
                        # Not sure how many of them are important here -- all except mode?
                        im.defineimage(
                            nx=4096,
                            ny=4096,
                            cellx='2arcsec',
                            celly='2arcsec',
                            mode='mfs',
                            facets=1
                        )
                        im.setoptions(ftmachine='wproject', wprojplanes=w_projection_planes, freqinterp='linear')
                        im.settaylorterms(ntaylorterms=1)
                        #
                        print 'Model in this pass: ' + str(ha_model[nmodel])
                        print 'Time range in this pass: ' + timerange
                        tb.open(ha_model[nmodel])
                        tb.clearlocks()
                        tb.close()
                        #
                        im.ft(model=ha_model[nmodel], incremental=False) # not_first)
                        not_first=True
                    # if samples in this HA range
                # next HA m
                im.close()
                uvsub(vis=tmp_name2, reverse=False)
                os.system('rm -r %s'%(tmp_name))
                os.system('rm -r %s'%(tmp_name1))
                split(vis=tmp_name2, outputvis=tmp_name1, datacolumn='corrected')
                if out_rot_data == 'yes':
                    split(vis=tmp_name1,datacolumn='data',width=64,timebin='30s',
                          outputvis='%s/O%d_%s'%(png_directory,nmodel,out_ms))
                fixvis(vis=tmp_name1,outputvis=tmp_name,phasecenter=ms_phasecentre)   
            # End of run through outlier models
            if out_pngs == 'yes':
                ret_d = plotms(vis=tmp_name1,xaxis='freq',yaxis='real',avgtime='43200',overwrite=True,avgbaseline=True,showgui=False,ydatacolumn='data',xdatacolumn='data',plotfile=png_directory+'/'+in_dir.rsplit('/')[-1]+'_outfield_subtraction_data.png')
                ret_m = plotms(vis=tmp_name1,xaxis='freq',yaxis='real',avgtime='43200',overwrite=True,avgbaseline=True,showgui=False,ydatacolumn='model',xdatacolumn='model',plotfile=png_directory+'/'+in_dir.rsplit('/')[-1]+'_outfield_subtraction_model.png')
                ret_c = plotms(vis=tmp_name1,xaxis='freq',yaxis='real',avgtime='43200',overwrite=True,avgbaseline=True,showgui=False,ydatacolumn='corrected',xdatacolumn='corrected',plotfile=png_directory+'/'+in_dir.rsplit('/')[-1]+'_outfield_subtraction_corrected.png')
                if (ret_d&ret_c&ret_m) == False:
                    print 'Reporting Outlier PlotMS Failure! State for Data, Corrected and Model is: '+str(ret_d)+'&'+str(ret_c)+'&'+str(ret_m)
            #Could be a copy
            split(vis=tmp_name,outputvis=os.path.join(out_dir, out_ms),datacolumn='data')
        else:
            split(vis=in_dir, outputvis=os.path.join(out_dir, out_ms), datacolumn='corrected')
        if sub_uzero == True:
            tb.open(os.path.join(out_dir, out_ms),nomodify=False)
            tq=tb.query('',columns='UVW,FLAG')
            uv=tq['UVW']
            fg=tq['FLAG'].T
            I=np.where(np.abs(uv[0])<10)[0]
            print('Flagging %d baselines on %s, on which u is ~zero'%(len(I),os.path.join(out_dir, out_ms)))
            fg[I]=True
            tb.putcol('FLAG',fg.T)
            tb.close()

except Exception:
        LOG.exception('*********\nUVSub exception: \n***********')


if __name__ == "__main__":
    args = parse_args()
    LOG.info(args)

    do_uvsub(
            in_dir=args.arguments[0],
            out_dir=args.arguments[1],
            out_ms=args.arguments[2],
            out_pngs=args.arguments[3],
            w_projection_planes=int(args.arguments[4]),
            number_taylor_terms=int(args.arguments[5]),
            model=args.arguments[6:])
