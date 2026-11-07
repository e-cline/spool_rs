// Futures holding `Rc` and `RefCell` across `.await` can be spawned.
use std::cell::RefCell;
use std::rc::Rc;

fn main() {
    spool::block_on(async {
        let shared = Rc::new(RefCell::new(0));
        let s = shared.clone();
        let handle = spool::spawn(async move {
            spool::yield_now().await;
            *s.borrow_mut() += 1;
            s
        });
        let back: Rc<RefCell<i32>> = handle.await;
        assert_eq!(*back.borrow(), 1);
    });
}
