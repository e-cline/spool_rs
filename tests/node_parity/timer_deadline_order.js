// Twin of `timer_deadline_order` in tests/node_parity.rs.
async function main() {
  for (const [label, ms] of [["x40", 40], ["y10", 10], ["z10", 10], ["w25", 25]]) {
    setTimeout(() => console.log(label), ms);
  }
}
main();
