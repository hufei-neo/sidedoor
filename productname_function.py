import re


def fill_na(df):
    df = df.fillna('').astype(str)
    return df


def remove_char(item_name):
    # remove 5x5 oz or 15 oz or 5x5 fl.oz or 15 fl.oz or 0.55oz or 3pack or 3
    # pack #0874  Pack Of 3 bottle……
    pattern = re.compile(
        r"(pack of\s?[0-9]*)",
        flags=re.I)  # remove like Pack Of
    item_name = re.sub(pattern, ' ', item_name)

    pattern = re.compile(r"([0-9]*\s?{}s?)".format('bottle'),  # remove like 3 bottle
                         flags=re.I)
    item_name = re.sub(pattern, ' ', item_name)

    filter_list = [
        'oz',
        'fl oz',
        'fl.oz',
        'pack',
        'count',
        'lb',
        'ct',
        'iu',
        'lu',
        'billion',
        'mg',
        'pound',
        'mcg',
        'ounce',
        'fl',
        'fz',
        'grams',
        'pc',
        'g',
        'ea',
        'ml',
        'es']
    for filter_i in filter_list:
        pattern = re.compile(r"([0-9\,x]*[0-9.]+\s?{}s?)".format(filter_i),
                             flags=re.I)
        item_name = re.sub(pattern, '', item_name)

    pattern = re.compile(r"([0-9\/\s\-]+{}s?)".format('et'),
                         flags=re.I)
    item_name = re.sub(pattern, '', item_name)

    pattern = re.compile(r"([0-9\/\s\-]+{}s?)".format('ule'),
                         flags=re.I)
    item_name = re.sub(pattern, '', item_name)

    pattern = re.compile(r"(\#[0-9a-z]*)", flags=re.I)  # remove like #0874
    item_name = re.sub(pattern, '', item_name)

    pattern = re.compile(
        r"(size:[\s0-9]*)",
        flags=re.I)  # remove like size:  %c2%ae
    item_name = re.sub(pattern, '', item_name)

    pattern = re.compile(
        r"(\%[0-9a-z]*\%[a-z0-9]*)",
        flags=re.I)  # remove like %c2%ae
    item_name = re.sub(pattern, '', item_name)

    pattern = re.compile(
        r"([a-z0-9]*\![a-z0-9]*)",
        flags=re.I)  # remove like  abc!
    item_name = re.sub(pattern, '', item_name)

    # replace 特殊符号
    filter_chr = ['®', '™', '®']
    for i in filter_chr:
        item_name = item_name.lower().replace(i, '')

    def cap_all(s):
        filter_ = [
            '',
            '-',
            '--',
            ';',
            '|',
            'x',
            '~',
            's',
            '/',
            '.',
            'Other',
            '!',
            "'s"]
        filter_ += filter_list
        return ' '.join([x.capitalize()
                         for x in s.split(' ') if x not in filter_])

    return cap_all(item_name)


def clear_item_name(sers):
    """clear the item_name
    1. drop the brand name from item_name
    2. drop the pkg and unit from item_name
    3. drop the prim_ingr_dosage and dosage_unit from item_name
    4. drop the weight_metric and weight_metric_unit from item_name
    5. drop the format_ and plus format_std from item_name
    6. drop the (.*?) from item_name
    :param sers:
    :return:
    """
    item_name = sers.at['ecomItemName']
    ecom_brand = sers.at['ecombrand']
    mstr_brand_name = sers.at['mstrBrandName']
    pkg_qty = sers.at['pkgQty']
    pkg_qty_unit = sers.at['pgkQtyUnit']
    prim_ingr_dosage = sers.at['primIngrDosage']
    dosage_unit = sers.at['dosageUnit']
    weight_metric = sers.at['weightMetric']
    weight_metric_unit = sers.at['weightMetricUnit']
    weight_imperial = sers.at['weightImperial']
    weight_imperial_unit = sers.at['weightImperialUnit']
    format_ = sers.at['format']
    format_std = sers.at['formatStd']

    weight_metric_unit = re.sub(r'\(|\)', '', weight_metric_unit)
    item_name = re.sub(r'\(|\)', '', item_name)

    item_name = item_name.lower().replace(ecom_brand.lower(), '')
    item_name = item_name.lower().replace(mstr_brand_name.lower(), '')

    if pkg_qty_unit != '':
        pattern = re.compile(
            r"([0-9,\.,]*[0-9]+\s?-?{}s?)".format(pkg_qty_unit),
            flags=re.I)
        item_name = re.sub(pattern, '', item_name)
    if dosage_unit != '':
        pattern = re.compile(
            r"([0-9,\.,]*[0-9]+\s?-?{}s?)".format(dosage_unit),
            flags=re.I)
        item_name = re.sub(pattern, '', item_name)
    if weight_metric_unit != '':
        pattern = re.compile(
            r"([0-9,\.,]*[0-9]+\s?-?{}s?)".format(weight_metric_unit),
            flags=re.I)
        item_name = re.sub(pattern, '', item_name)
    if weight_imperial_unit != '':
        pattern = re.compile(
            r"([0-9,\.,]*[0-9]+\s?-?{}s?)".format(weight_imperial_unit),
            flags=re.I)
        item_name = re.sub(pattern, '', item_name)

    if format_ != '':
        pattern = re.compile(r"([0-9.,]*\s{}s?)".format(format_), flags=re.I)
        item_name = re.sub(pattern, ' ', item_name)

    if format_std != '' and format_std.lower() not in item_name:
        item_name = item_name + ' ' + format_std

    pattern = re.compile(r'\(.*?\)', flags=re.I)
    item_name = re.sub(pattern, '', item_name)

    pattern = re.compile(r'\[.*?\]', flags=re.I)
    item_name = re.sub(pattern, '', item_name)

    item_name = item_name.replace(',', '').strip()

    return remove_char(item_name)


def compare_name(sers):
    item_name_slug = sers.at['item_name_slug']
    name_new = sers.at['name']
    format_ = sers.at['format']
    format_std = sers.at['formatStd']
    ecom_brand = sers.at['ecombrand']
    mstr_brand_name = sers.at['mstrBrandName']

    if name_new != '' and (item_name_slug == '' or (
            item_name_slug != '' and len(item_name_slug) >= len(name_new))):

        return name_new
    else:
        item_name_slug = item_name_slug.lower().replace(ecom_brand.lower(), '')
        item_name_slug = item_name_slug.lower().replace(mstr_brand_name.lower(), '')
        if format_ != '':
            pattern = re.compile(
                r"([0-9.,]*\s?{}s?)".format(format_), flags=re.I)
            item_name_slug = re.sub(pattern, ' ', item_name_slug)
        if format_std != '' and format_std.lower() not in item_name_slug:
            item_name_slug = item_name_slug + ' ' + format_std
        return remove_char(item_name_slug)


def parse_slug(sers):
    """Convert a url into a raw name."""
    url = sers.at['ecomItemUrl']
    url_headers = ["https://www.amazon.com/",
                   # name before '--', + brand rm
                   "https://www.vitacost.com/",
                   "https://www.iherb.com/pr/",
                   "https://www.walmart.com/ip/",
                   "https://www.vitaminshoppe.com/p/",
                   # brand rm, otherwise name "good enough"
                   "https://www.walgreens.com/store/c/",
                   # this gives an id, but names are alr "good enough" + brand rm
                   # "https://www.gnc.com/",
                   # need to remove 3 categories first, then match, postfix "-prodid"
                   # "https://www.cvs.com/shop/",
                   # names are "good", still need parsed before 1st '-', brand remove
                   "https://www.target.com/p/",
                   # names are "good enough"
                   "http://www.naturemade.com/",
                   ]
    if url.startswith("https://www.gnc.com/"):
        return None
    elif url.startswith("https://www.cvs.com/shop/"):
        url = url.replace("https://www.cvs.com/shop/", "")
        categs_pattern = re.compile("[^/]+/[^/]+/[^/]+/")
        slug_pattern = re.compile(r"[a-zA-Z0-9\-\%\'\!]+(?=-prodid)")

        url = re.sub(categs_pattern, "", url)
        match = re.match(slug_pattern, url)
    else:
        for header in url_headers:
            url = url.replace(header, "")
        slug_pattern = re.compile(r"[a-zA-Z0-9\-\%\'\!]+(?=\/|$)")
        match = re.match(slug_pattern, url)

    if match:
        match_text = match.group().replace("-", " ").replace("_", " ").lower()
        if not match_text == "dp":
            return match_text


"""
df_all=df_all.pipe(fill_na).assign(name=lambda d: d.apply(clear_item_name, axis=1)).\
assign(item_name_slug=lambda d: d.apply(parse_slug, axis=1)).pipe(fill_na).\
assign(name_final=lambda d: d.apply(compare_name, axis=1))
name                 -------to do cluster
item_name_slug          -------url_slug
name_final             -------compare with  name and item_name_slug to choose one better
"""
