from jinja2 import Template

class Base(object):
  def __init__(self, label, width):
    self.is_enum = False
    self.is_text = False
    self.is_file = False
    self.label = label
    self.width = width

class Enumeration(Base):
  def __init__(self, label, values, allow_multiple, width=2):
    Base.__init__(self, label, width)
    self.is_enum = True
    self.values = values
    self.allow_multiple = allow_multiple

class Text(Base):
  def __init__(self, label, default="", width=2):
    Base.__init__(self, label, width)
    self.is_text = True
    self.default = default

class FileUpload(Base):
    def __init__(self, label, width=2):
        Base.__init__(self, label, width)
        self.is_file = True

class Form:
    def __init__(self, template, *fields):
        self.fields = fields
        self.template = template

ATLANTIS_FORM = Form("atlantis.flock",
  Text("repo", default="ssh://git@stash.broadinstitute.org:7999/cpds/atlantis.git", width=6),
  Text("branch", default="refs/heads/master"),
  Enumeration("targetDataset", ["ach2.12", "55k", "98k"], False),
  Enumeration("targetDataType", ["gene solutions", "seed solutions"], False),
  Enumeration("celllineSubset", ["all", "solid"], False),
  Enumeration("predictiveFeatures", ["GE", "CN", "MUT", "SI", "miRNA", "high conf GS"], True),
  Enumeration("predictiveFeatureSubset", ["single", "all"], False)
)

GENERIC_FORM = Form("generic.flock",
  Text("repo", default="ssh://git@stash.broadinstitute.org:7999/cpds/atlantis.git", width=6),
  Text("branch", default="refs/heads/master"),
  FileUpload("config", width=6)
)

def apply_parameters(template, params):
    with open(template) as fd:
        return Template(fd.read()).render(**params)
