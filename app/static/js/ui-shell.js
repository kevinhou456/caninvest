/* Progressive keyboard and mobile navigation for the investment shell. */
(() => {
    'use strict';
    const sidebar = document.getElementById('family-sidebar');
    const toggle = document.getElementById('sidebar-toggle');
    if (!sidebar || !toggle) return;
    const backdrop = document.getElementById('sidebar-backdrop');
    const content = document.getElementById('main-content');
    const mobile = window.matchMedia('(max-width: 991px)');
    const items = sidebar.querySelectorAll('.nav-item[onclick]');
    items.forEach(item => {
        item.setAttribute('role', 'link');
        item.tabIndex = 0;
        if (item.classList.contains('active')) item.setAttribute('aria-current', 'page');
        item.addEventListener('keydown', event => {
            if (event.key === 'Enter' || event.key === ' ') {
                event.preventDefault();
                item.click();
            }
        });
    });
    document.querySelector('.top-nav .active')?.setAttribute('aria-current', 'page');
    function setOpen(open, restoreFocus = false) {
        open = mobile.matches && open;
        document.body.classList.toggle('sidebar-open', open);
        toggle.setAttribute('aria-expanded', String(open));
        sidebar.inert = mobile.matches && !open;
        content.inert = open;
        if (open) (sidebar.querySelector('[aria-current="page"]') || items[0])?.focus();
        else if (restoreFocus) toggle.focus();
    }
    toggle.addEventListener('click', () => setOpen(toggle.getAttribute('aria-expanded') !== 'true', true));
    backdrop.addEventListener('click', () => setOpen(false, true));
    document.addEventListener('keydown', event => {
        if (!document.body.classList.contains('sidebar-open')) return;
        if (event.key === 'Escape') { setOpen(false, true); return; }
        if (event.key !== 'Tab') return;
        const focusable = [toggle, ...sidebar.querySelectorAll('a[href], button:not([disabled]), [tabindex="0"]')];
        const first = focusable[0], last = focusable[focusable.length - 1];
        if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
        else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
    });
    mobile.addEventListener('change', () => setOpen(false));
    setOpen(false);
})();
