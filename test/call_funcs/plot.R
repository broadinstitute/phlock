plotClass <- function(per.task.state, common.state, output.file, run) {
  className <- per.task.state
  rows <- common.state[common.state$class == className, ]
  pdf(paste(run$results, "/", className, ".pdf", sep=''))
  str(common.state)
  str(per.task.state)
  print(rows$s.wid)
  print(rows$s.len)
  str(rows)
  plot(rows$s.wid, rows$s.len)
  dev.off()
}
