SELECT tagid, intvalue, floatvalue, stringvalue, datevalue, t_stamp, tagpath,
tagpath as deviceid from sqlt_data_1_%04d_%02d as data, sqlth_te as tag where data.tagid = tag.id 
and t_stamp >= %d and t_stamp < %d
