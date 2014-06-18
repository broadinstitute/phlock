class Base(object):
  def __init__(self, label):
    self.is_enum = False
    self.is_text = False
    self.label = label

class Enumeration(Base):
  def __init__(self, label, values, allow_multiple):
    Base.__init__(self, label)
    self.is_enum = True
    self.values = values
    self.allow_multiple = allow_multiple

class Text(Base):
  def __init__(self, label):
    Base.__init__(self, label)
    self.is_text = True

ATLANTIS_FORM = [
  Enumeration("targetDataType", ["gene solutions", "seed solutions"], False),
  Enumeration("celllineSubset", ["all", "solid"], False),
  Enumeration("predictiveFeatures", ["GE", "CN", "Mut", "SI", "gene solutions"], True),
  Enumeration("predictiveFeatureSubset", ["single", "top100", "all"], False)
]
