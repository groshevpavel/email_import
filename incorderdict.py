from collections import OrderedDict
class IncrementalOrderedDict(OrderedDict):
    """
        При добавлении ключа который уже есть в словаре, 
        происходит перевод значения в список и новый элемент добавляется в этот список
    """
    def __init__(self, *args, **kw):
        super(__class__, self).__init__(*args, **kw)
        self.keyname=kw.get("keyname","ключа(ей)")
        self.valuename=kw.get("valuename","значения")

    def __getitem__(self, key):
        """
            ВНИМАНИЕ!!!
            Возврат значения производится по ЧАСТИ названия ключа!
        """
        if key in self:
            return super(__class__, self).__getitem__(key)
        else:
            for k,v in super(__class__, self).items():
                if key in k:
                    return v

    def __setitem__(self, key, val):
        if key in self:
            vval = super(__class__, self).__getitem__(key)
            
            if not isinstance(vval, (list, tuple,)):
                v = [vval]
                v.append(val)
                super(__class__, self).__setitem__(key, v)
            else:
                vval.append(val)
                super(__class__, self).__setitem__(key, vval)
                
        else:
            super(__class__, self).__setitem__(key, val)

    def __str__(self):
        s=f"список: "
        s += "; ".join([f"{self.valuename}:{k}, {self.keyname}:{','.join([str(vv) for vv in [v]])}" for k,v in self.items()])
        return s
