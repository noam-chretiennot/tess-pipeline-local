"""
Model and Parser for the metadata of TESS files.

See documentation at https://archive.stsci.edu/missions/tess/doc/EXP-TESS-ARC-ICD-TM-0014.pdf#page=17
"""

import re
from datetime import datetime
from pydantic import BaseModel
from astropy.io import fits

class AstroFileMetadata(BaseModel):
    filename: str
    ap_0_1: float
    ap_0_2: float
    ap_0_3: float
    ap_0_4: float
    ap_1_0: float
    ap_1_1: float
    ap_1_2: float
    ap_1_3: float
    ap_2_0: float
    ap_2_1: float
    ap_2_2: float
    ap_3_0: float
    ap_3_1: float
    ap_4_0: float
    ap_order: int
    a_0_2: float
    a_0_3: float
    a_0_4: float
    a_1_1: float
    a_1_2: float
    a_1_3: float
    a_2_0: float
    a_2_1: float
    a_2_2: float
    a_3_0: float
    a_3_1: float
    a_4_0: float
    a_dmax: float
    a_order: int
    backapp: str
    barycorr: float
    bfrowe: int
    bfrows: int
    bitpix: int
    bjdreff: float
    bjdrefi: int
    bp_0_1: float
    bp_0_2: float
    bp_0_3: float
    bp_0_4: float
    bp_1_0: float
    bp_1_1: float
    bp_1_2: float
    bp_1_3: float
    bp_2_0: float
    bp_2_1: float
    bp_2_2: float
    bp_3_0: float
    bp_3_1: float
    bp_4_0: float
    bp_order: int
    btc_pix1: float
    btc_pix2: float
    bunit: str
    b_0_2: float
    b_0_3: float
    b_0_4: float
    b_1_1: float
    b_1_2: float
    b_1_3: float
    b_2_0: float
    b_2_1: float
    b_2_2: float
    b_3_0: float
    b_3_1: float
    b_4_0: float
    b_dmax: float
    b_order: int
    camera: int
    ccd: int
    cd1_1: float
    cd1_2: float
    cd2_1: float
    cd2_2: float
    cdelt1p: float
    cdelt2p: float
    checksum: str
    crpix1: float
    crpix1p: int
    crpix2: float
    crpix2p: int
    crval1: float
    crval1p: int
    crval2: float
    crval2p: int
    ctype1: str
    ctype1p: str
    ctype2: str
    ctype2p: str
    cunit1p: str
    cunit2p: str
    date_end: datetime
    date_obs: datetime
    deadapp: str
    deadc: float
    dec_nom: float
    dquality: int
    equinox: float
    exposure: float
    extname: str
    extver: int
    frametim: float
    fxdoff: int
    gaina: float
    gainb: float
    gainc: float
    gaind: float
    gcount: int
    imagtype: str
    inherit: str
    instrume: str
    int_time: float
    livetime: float
    lvcea: int
    lvceb: int
    lvcec: int
    lvced: int
    lvcsa: int
    lvcsa: int
    lvcsb: int
    lvcsb: int
    lvcsc: int
    lvcsc: int
    lvcsc: int
    lvcsc: int
    meanblca: int
    meanblcb: int
    meanblcc: int
    meanblcd: int
    naxis: int
    naxis1: int
    naxis2: int
    nreadout: int
    num_frm: int
    pcount: int
    radesys: str
    ra_nom: float
    readnoia: float
    readnoib: float
    readnoic: float
    readnoid: float
    readtime: float
    roll_nom: float
    sccea: int
    scceb: int
    sccec: int
    scced: int
    sccsa: int
    sccsb: int
    sccsc: int
    sccsd: int
    scicolha: str
    scicolhb: str
    scicolhc: str
    scicolhd: str
    scirowe: int
    scirows: int
    simdata: str
    smrowe: int
    smrows: int
    tassign: str
    telapse: float
    telescop: str
    tierrela: float
    timedel: float
    timepixr: float
    timeref: str
    timesys: str
    timeunit: str
    tstart: float
    tstop: float
    tvcea: int
    tvceb: int
    tvcec: int
    tvced: int
    tvcsa: int
    tvcsa: int
    tvcsb: int
    tvcsb: int
    tvcsc: int
    tvcsc: int
    tvcsc: int
    tvcsc: int
    vignapp: str
    vrowe: int
    vrows: int
    wcsaxes: int
    wcsaxesp: int
    wcsnamep: str
    xtension: str

    @staticmethod
    def str_to_key_value(string: str):
        """Convert a FITS header line to a formatted key-value pair."""
        key_value = string.split('/')[0] \
            .replace(' ', '') \
            .replace('\'', '') \
            .split("=")
        if len(key_value) != 2:
            return None

        key_value[1] = key_value[1].lower()
        key_value[0] = key_value[0].lower()\
            .replace('-', '_')

        # Check if value is a number (including scientific notation)
        if re.match(r'^-?\d+(?:\.\d+)?(?:e[+-]?\d+)?$', key_value[1]):
            key_value[1] = float(key_value[1])
        elif re.match(r'^-?\d+$', key_value[1]):
            key_value[1] = int(key_value[1])
        return key_value

    @classmethod
    def Parse_fits_header(cls, header: str, filename:str=""):
        """Parse the FITS header and return an AstroFileMetadata object."""
        header_lines = header.split('\n')

        # Format the key-value pairs
        header_json = list(map(cls.str_to_key_value, header_lines))[:-1]

        header_dict = dict(header_json)
        header_dict['filename'] = filename
        return cls(**header_dict)

    @classmethod
    def Parse_fits_file(cls, file: fits.hdu.hdulist.HDUList, filename:str=""):
        """Parse a FITS file and return an AstroFileMetadata object."""
        hdu = file[1] # ignore the primary header

        return cls.Parse_fits_header(hdu.header.tostring(sep='\n'), filename)
